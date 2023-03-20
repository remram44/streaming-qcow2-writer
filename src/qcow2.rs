use byteorder::{BigEndian, WriteBytesExt};
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;

const CLUSTER_SIZE: u64 = 65536;

const REPORT_INTERVAL_BYTES: u64 = 500_000_000; // 500 MB

pub struct StreamingQcow2Writer {
    input_size: u64,
    l1_clusters: u32,
    l1_offset: u64,
    refcount_table_clusters: u32,
    first_data_cluster: u64,
    data_clusters: Vec<u64>,
}

fn divide_and_round_up(a: u64, b: u64) -> u64 {
    (a + b - 1) / b
}

impl StreamingQcow2Writer {
    pub fn new<I: Iterator<Item=Range<u64>>>(input_size: u64, ranges: I) -> StreamingQcow2Writer {
        // Build a list of clusters
        let mut data_clusters = Vec::new();
        let mut last_cluster = None;
        for range in ranges {
            // Compute the range of clusters containing those bytes
            let mut from_cluster = range.start / CLUSTER_SIZE;
            let to_cluster = divide_and_round_up(range.end, CLUSTER_SIZE);

            if let Some(last_cluster) = last_cluster {
                if from_cluster < last_cluster {
                    panic!("Data clusters are not sorted");
                } else if from_cluster == last_cluster {
                    // It is possible for the start of this range to fall in
                    // the same cluster where the last range ended
                    from_cluster += 1;
                }
            }
            last_cluster = Some(to_cluster - 1);

            // Add each cluster to the list
            for cluster in from_cluster..to_cluster {
                data_clusters.push(cluster);
            }
        }

        // Compute the number of L2 tables required
        let guest_clusters = divide_and_round_up(input_size, CLUSTER_SIZE);
        let l2_tables = divide_and_round_up(guest_clusters * 8, CLUSTER_SIZE);

        // Compute the size of the L1 table in clusters
        let l1_clusters = divide_and_round_up(l2_tables * 8, CLUSTER_SIZE);

        // Picking a number of refcount blocks changes the number of allocated
        // clusters, which changes the number of refcount blocks
        let mut refcount_blocks = 1;
        let mut refcount_table_clusters = 1;
        loop {
            let total_clusters =
                1 // Header
                + refcount_table_clusters
                + refcount_blocks
                + l1_clusters
                + l2_tables
                + data_clusters.len() as u64; // Data
            let new_refcount_blocks = divide_and_round_up(total_clusters * 2, CLUSTER_SIZE);
            if new_refcount_blocks == refcount_blocks {
                break;
            }
            refcount_blocks = new_refcount_blocks;
            refcount_table_clusters = divide_and_round_up(refcount_blocks * 8, CLUSTER_SIZE);
        }

        let l1_offset = CLUSTER_SIZE * (
            1 // Header
            + refcount_table_clusters
            + refcount_blocks
        );

        let first_data_cluster =
            1 // Header
            + refcount_table_clusters
            + refcount_blocks
            + l1_clusters
            + l2_tables;

        StreamingQcow2Writer {
            input_size,
            l1_clusters: l1_clusters as u32,
            l1_offset,
            refcount_table_clusters: refcount_table_clusters as u32,
            first_data_cluster,
            data_clusters,
        }
    }

    fn total_clusters(&self) -> u64 {
        self.first_data_cluster + self.data_clusters.len() as u64
    }

    pub fn file_size(&self) -> u64 {
        CLUSTER_SIZE * self.total_clusters()
    }

    pub fn total_guest_clusters(&self) -> u64 {
        divide_and_round_up(self.input_size, CLUSTER_SIZE)
    }

    pub fn write_header<W: Write>(&self, mut writer: W) -> std::io::Result<()> {
        // Magic
        writer.write_all(b"QFI\xFB")?;

        // Version
        writer.write_u32::<BigEndian>(2)?;

        // Backing file name offset (0 = no backing file)
        writer.write_u64::<BigEndian>(0)?;

        // Backing file name length
        writer.write_u32::<BigEndian>(0)?;

        // Number of bits per cluster address, 1<<bits is the cluster size
        assert_eq!(CLUSTER_SIZE, 1 << 16);
        writer.write_u32::<BigEndian>(16)?;

        // Virtual disk size in bytes
        writer.write_u64::<BigEndian>(self.input_size)?;

        // Encryption method (none)
        writer.write_u32::<BigEndian>(0)?;

        // L1 table size (number of entries)
        let l2_entries_per_cluster = CLUSTER_SIZE / 8;
        let l1_entries = self.total_guest_clusters() / l2_entries_per_cluster;
        writer.write_u32::<BigEndian>(l1_entries as u32)?;

        // L1 table offset
        writer.write_u64::<BigEndian>(self.l1_offset)?;

        // Refcount table offset
        writer.write_u64::<BigEndian>(CLUSTER_SIZE)?;

        // Refcount table length in clusters
        writer.write_u32::<BigEndian>(self.refcount_table_clusters)?;

        // Number of snapshots in the image
        writer.write_u32::<BigEndian>(0)?;

        // Offset of the snapshot table (must be aligned to clusters)
        writer.write_u64::<BigEndian>(0)?;

        writer.write_all(&[0u8; CLUSTER_SIZE as usize - 72])?;

        self.write_refcount_table(&mut writer)?;

        self.write_mapping_table(&mut writer)?;

        Ok(())
    }

    fn write_refcount_table<W: Write>(&self, mut writer: W) -> std::io::Result<()> {
        let refcount_blocks = divide_and_round_up(self.total_clusters() * 2, CLUSTER_SIZE);

        // Table
        {
            for block in 0..refcount_blocks {
                writer.write_u64::<BigEndian>(CLUSTER_SIZE * (
                    1
                    + self.refcount_table_clusters as u64
                    + block as u64
                ))?;
            }
            let refcount_entries_per_cluster = CLUSTER_SIZE / 8;
            let last_cluster_entries = refcount_blocks as u64 % refcount_entries_per_cluster;
            if last_cluster_entries > 0 {
                for _ in last_cluster_entries..refcount_entries_per_cluster {
                    writer.write_u64::<BigEndian>(0)?;
                }
            }
        }

        // Blocks
        {
            for _ in 0..self.total_clusters() {
                writer.write_u16::<BigEndian>(1)?;
            }
            let block_entries_per_cluster = CLUSTER_SIZE / 2;
            let last_cluster_entries = self.total_clusters() % block_entries_per_cluster;
            if last_cluster_entries > 0 {
                for _ in last_cluster_entries..block_entries_per_cluster {
                    writer.write_u16::<BigEndian>(0)?;
                }
            }
        }

        Ok(())
    }

    fn write_mapping_table<W: Write>(&self, mut writer: W) -> std::io::Result<()> {
        // Build the mapping from guest to host
        let mut mapping = HashMap::new();
        for (host, guest) in self.data_clusters.iter().enumerate() {
            mapping.insert(guest, host as u64 + self.first_data_cluster);
        }

        // L1 table
        {
            let l1_entries_per_cluster = CLUSTER_SIZE / 8;
            let l1_entries = divide_and_round_up(self.total_guest_clusters(), l1_entries_per_cluster);
            for entry in 0..l1_entries {
                let offset =
                    self.l1_offset
                    + self.l1_clusters as u64 * CLUSTER_SIZE
                    + entry * CLUSTER_SIZE;
                let l1_entry = offset | (1 << 63);
                writer.write_u64::<BigEndian>(l1_entry)?;
            }

            let last_cluster_entries = l1_entries % l1_entries_per_cluster;
            if last_cluster_entries > 0 {
                for _ in last_cluster_entries..l1_entries_per_cluster {
                    writer.write_u64::<BigEndian>(0)?;
                }
            }
        }

        // L2 table
        {
            for guest_cluster in 0..self.total_guest_clusters() {
                let l2_entry = match mapping.get(&guest_cluster) {
                    None => {
                        0
                    }
                    Some(host_cluster) => {
                        let offset = host_cluster * CLUSTER_SIZE;
                        offset
                            | (0 << 62) // Standard cluster
                            | (1 << 63) // Standard cluster with refcount=1
                    }
                };
                writer.write_u64::<BigEndian>(l2_entry)?;
            }

            let l2_entries_per_cluster = CLUSTER_SIZE / 8;
            let last_cluster_entries = self.total_guest_clusters() % l2_entries_per_cluster;
            if last_cluster_entries > 0 {
                for _ in last_cluster_entries..l2_entries_per_cluster {
                    writer.write_u64::<BigEndian>(0)?;
                }
            }
        }

        Ok(())
    }

    pub fn copy_data<R: Read + Seek, W: Write>(&self, mut reader: R, mut writer: W) -> std::io::Result<()> {
        let mut written = self.first_data_cluster * CLUSTER_SIZE;
        for cluster in &self.data_clusters {
            reader.seek(SeekFrom::Start(cluster * CLUSTER_SIZE))?;
            let mut buffer = [0u8; CLUSTER_SIZE as usize];
            reader.read(&mut buffer)?;
            writer.write_all(&buffer)?;

            if (written + CLUSTER_SIZE) / REPORT_INTERVAL_BYTES
                != written / REPORT_INTERVAL_BYTES
            {
                eprintln!("{}/{} bytes written", written + CLUSTER_SIZE, self.file_size());
            }
            written += CLUSTER_SIZE;
        }

        Ok(())
    }
}
