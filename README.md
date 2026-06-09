# OCR-vx

This repository contains the **OCR-vx** runtime implementations, originally developed at the
Research Group Scientific Computing, University of Vienna.

**Project website:** https://ocr-vx.univie.ac.at/

---

## Origin and Authorship

This code is the unmodified source downloaded from the OCR-vx project website.

### OCR-vx implementations (`src/`, `include/common/`, `include/one/`, `include/shmem/`, `include/distributed/`)

Copyright (c) 2015–2018, Research Group Scientific Computing, University of Vienna.
Licensed under the terms in `LICENSE_UNIVIE`.

**Author:** Jiri Dokulil (University of Vienna)

### OCR API specification and headers (`include/ocr/`)

Copyright (c) 2012–2015, Intel Corporation & Rice University.
Licensed under the terms in `LICENSE`.

**Contributors:**
- Zoran Budimlic (Rice University)
- Vincent Cave (Rice University)
- Sanjay Chatterjee (Rice University / Intel Corporation)
- Romain Cledat (Intel Corporation)
- Joshua Fryman (Intel Corporation)
- Ivan Ganev (Intel Corporation)
- Samkit Jain (Intel Corporation)
- Robin Knauerhase (Intel Corporation)
- Min Lee (Intel Corporation)
- Timothy Mattson (Intel Corporation)
- Brian Nickerson (Intel Corporation)
- Nick Pepperling (Intel Corporation)
- Vivek Sarkar (Rice University)
- Bala Seshasayee (Intel Corporation)
- Sagnak Tasirlar (Rice University / Intel Corporation)
- Rob Van der Wijngaart (Intel Corporation)
- Jiri Dokulil (University of Vienna)

---

## Implementations

OCR-vx provides five build configurations across three runtime families:

| Build directory | Library | OCR type | Description |
|---|---|---|---|
| `build_one/` | `ocr_one` | v1 | Single-process, no TBB |
| `build_shm/` | `ocr_tbb` | vsm | Single-node, TBB scheduler |
| `build_distributed_fake/` | `ocr_vdm_fake` | vdm | Multi-node simulated (single process) |
| `build_distributed_mpi/` | `ocr_vdm_mpi` | vdm | Multi-node via MPI |
| `build_distributed_sock/` | `ocr_vdm_sock` | vdm | Multi-node via ZMQ sockets |

The three `distributed` variants compile the same source under `src/distributed/`; they differ
only in the transport backend selected at compile time
(`SIMULATE_MULTIPLE_NODES` / `OCR_USE_MPI` / `OCR_USE_SOCK`).

---

## License

Files under `include/ocr/` are covered by `LICENSE` (Intel Corporation & Rice University, BSD-style).
All other files are covered by `LICENSE_UNIVIE` (University of Vienna, BSD-style).
