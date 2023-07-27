# sotastream: regression tests

## Introduction

The framework has been adopted from [Marian NMT regression
tests](https://github.com/marian-nmt/marian-regression-tests).
A regression test is a bash script with file prefix name `test_*` in `tests`
directory that calls sotastream and produces outputs that is then compared
against the expected output.

## Usage

After setting up sotastream, download the data used in regression tests:

```bash
export AZURE_STORAGE_SAS_TOKEN='paste-here-a-valid-sas-token'
bash setup.sh
```

This needs to be done once after new data is added. The SAS token for
`https://romang.blob.core.windows.net/augmentibatch` (Internal Consumption) you
can generate yourself from Azure Portal or ask _rogrundk_. To do this,

1. Navigate to portal.azure.com
2. Find romang's storage account
3. Click on "Containers" in the lefthand navbar
4. Click on "augmentibatch" in the file navigator
5. Click on "Shared access tokens" in the lefthand navbar
6. Make sure you have "read" and "list" permissions
7. Click "Generate SAS token and URL"

Instead, you can also download the `data` folder manually via Microsoft Azure Storage Explorer.

Then run all tests:

```bash
bash run.sh
```

This will display an output similar to this:

```
[02/15/2023 03:04:47] Running on mt-gpu-008 as process 459328
[02/15/2023 03:04:47] Python: python3
[02/15/2023 03:04:47] Augmentibatch dir: /home/rogrundk/train/sotastream
[02/15/2023 03:04:47] Data dir: /home/rogrundk/train/sotastream/test/regression/data
[02/15/2023 03:04:47] Time out: 5m
[02/15/2023 03:04:47] Checking directory: tests
[02/15/2023 03:04:48] Checking directory: tests/basic
[02/15/2023 03:04:48] Running tests/basic/test_default_pipeline.sh ... OK
[02/15/2023 03:04:49] Test took 00:00:1.320s
[02/15/2023 03:04:49] Running tests/basic/test_t1_pipeline.sh ... OK
[02/15/2023 03:04:51] Test took 00:00:1.902s
---------------------
Ran 2 tests in 00:00:3.493s, 2 passed, 0 skipped, 0 failed
OK
```

See `run.sh` for more exacution examples and command-line arguments.

By default the script travers all subdirectories of `tests` and runs each bash
script with the file name format of `test_*.sh`.  Directories and files with
`_` prefix are ignored.  Detailed outputs for each test are stored in
`test_*.sh.log`.

## Adding new regression tests

See existing tests in `tests/basic` for examples.

