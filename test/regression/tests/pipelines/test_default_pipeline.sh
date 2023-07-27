#!/bin/bash

#####################################################################
# SUMMARY: Run the default pipeline with default options
#####################################################################

# Exit on error
set -eo pipefail

# Test command
$AB_PYTHON -B -m sotastream --seed 1111 -b 1 -n 1 \
        default $AB_DATA/wmt22.en-de.tsv.gz \
    | head -n 1000 > default.out

# Compare with the expected output
diff default.out default.expected > default.diff

# Exit with success code
exit 0
