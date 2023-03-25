# Streaming QCOW2 image writer

This is a tool that can write a QCOW2 image file in a streaming fashion. It can read a raw file or device and write a QCOW2 file, and contrary to `qemu-img convert`, it will not attempt to seek in the output.

Optionally it can consume a layout file in JSON format indicating which parts of the input file should be read; the other parts of the image will be assumed to be all zero and won't take space in the output.

The motivating use-case for this tool is backups: you can pipe the output to a backup system such as [Restic](https://restic.net/) without having to write the QCOW2 image to disk! The layout JSON matches the files given by `rbd diff` too, so you can do:

```console
$ rbd diff --whole-object --format=json my-vm-disk > my-vm-disk.json
$ rbd device map my-vm-disk
/dev/rbd0
$ streaming-qcow2-writer /dev/rbd0 my-vm-disk.json | restic -r /srv/restic-repo backup --stdin --stdin-filename=my-vm-disk.qcow2
$ rbd device unmap /dev/rbd0
```

## Features

* Writes QCOW files version 2.
* Uses the standard 65536-byte cluster size.
* No deduplication or skipping of zero blocks (the layout has to be computed ahead of time, we can't decide to write a smaller file after that). Let your backup system handle it, or sparsify your input layout first.
* No compression, so deduplication will work. On the flip side, the "restored" file will be bigger.
* Can read from either a regular file or a block device.
* Writes output file to stdout.
* Portable, although I don't know how you'd get a block device on Windows.
* Can be built as a static binary.
