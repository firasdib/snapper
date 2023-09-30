# :turtle: Snapper

Snapper is a simple python script that executes [SnapRAID](https://github.com/amadvance/snapraid) in order to sync and scrub the array. Inspired by the great [snapraid-aio-script](https://github.com/auanasgheps/snapraid-aio-script) with a limited feature set.

The reason I created this is that I wanted more granular control of how my setup worked, which consequently means, this script is opinionated.

## Features

- Sanity checks the array
- Runs `touch` if necessary
- Runs `diff` before attempting to `sync`
- Allows you to pre-hash before syncing
- Allows you to automatically re-run `sync` if snapraid recommends it
- Allows you to run snapraid with a lower priority to keep server and drives responsive
- Allows you to abort execution if configurable thresholds are broken
- Allows you to `scrub` after `sync`
- Logs the raw snapraid output as well as formatted text
- Creates a nicely formatted report and sends it via email or discord
- Provides live insight into the sync/scrub process in Discord

**This project is a work in progress, and can change at any time.**

I welcome bugfixes and contributions, but be aware that I will not merge PRs that I do not feel do not fit the usage of this tool.

## How to use

- Ensure you have Python 3.5 or later installed
- Install the necessary dependencies by running `pip3 install -r requirements.txt`
- Download the [latest release](https://github.com/firasdib/snapper/releases/latest) of this project, or clone the git project.
- Copy or rename `config.json.example` to `config.json`
- Run the script via `python3 snapper.py`

You may run the script with the `--force` flag to force a sync/scrub and ignore any thresholds or sanity checks.

## Configuration

A `config.json` file is required and expected to be in the same root as this script. 

Please read through the [json schema](config.schema.json) to understand the exact details of each property. If you're not fluent in json schema (I don't blame you), you could use something like [this](https://json-schema.app/view/%23?url=https%3A%2F%2Fraw.githubusercontent.com%2Ffirasdib%2Fsnapper%2Fmain%2Fconfig.schema.json) to get a better idea of the different options.
