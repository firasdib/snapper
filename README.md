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

**This project is a work in progress, and can change at any time.**

I welcome bugfixes and contributions, but be aware that I will not merge PRs that I do not feel do not fit the usage of this tool.

## How to use

- Ensure you have Python 3.5 or later installed
- Install the necessary dependencies by running `pip3 install -r requirements.txt`
- Download the [latest release](https://github.com/firasdib/snapper/releases) of this project, or clone the git project.
- Copy or rename `config.json.example` to `config.json`
- Run the script via `python3 snapper.py`

You may run the script with the `--force` flag to force a sync/scrub and ignore any thresholds or sanity checks.

## Configuration

A `config.json` file is required and expected to be in the same root as this script. 

The different values are explained below:

| Option                                 | Explanation                                                                                           | Type    |
|----------------------------------------|-------------------------------------------------------------------------------------------------------|---------|
| `snapraid.binary`                      | The location of your snapraid executable                                                              | String  |
| `snapraid.config`                      | Location of the snapraid config file. Necessary for sanity checks.                                    | String  |
| `snapraid.nice`                        | Run snapraid at a given `nice`. Default processes run at 0.                                           | Number  |
| `snapraid.diff.thresholds.added`       | If more files than the threshold amount have been added, don't execute jobs. Set to `0` to disable.   | Number  |
| `snapraid.diff.thresholds.removed`     | If more files than the threshold amount have been removed, don't execute jobs. Set to `0` to disable. | Number  |
| `snapraid.sync.pre_hash`               | Wheter to pre-hash changed blocks before syncing                                                      | Boolean |
| `snapraid.sync.auto_sync.enabled`      | Whether or not to re-run the sync command if snapraid recommends it                                   | Boolean |
| `snapraid.sync.auto_sync.max_attempts` | The max amount of attempts to `sync` the array before bailing.                                        | Number  |
| `snapraid.scrub.enabled`               | Whether or not to scrub the array.                                                                    | Boolean |
| `snapraid.scrub.check_percent`         | How many percent of the array to scrub each time. Set to `0` to disable scrubbing.                    | Number  |
| `snapraid.scrub.min_age`               | How old the blocks have to be before considered for scrub, in days.                                   | Number  |
| `snapraid.scrub.scrub_new`             | Whether to scrub new blocks or not.                                                                   | Boolean |
| `notifications.email.enabled`          | Whether or not to send notifications to the defined email                                             | Boolean |
| `notifications.email.binary`           | The location of `mailx`                                                                               | String  |
| `notifications.email.from_email`       | The senders email                                                                                     | String  |
| `notifications.email.to_email`         | The recipients email                                                                                  | String  |                                                       
| `notifications.discord.enabled`        | Whether or not to send notifications to Discord                                                       | Boolean |
| `notifications.discord.webhook_id`     | Discord webhook id                                                                                    | String  |
| `notifications.discord.webhook_token`  | Discord webhook token                                                                                 | String  |
| `logs.dir`                             | The directory in which to save logs. Will be created if it does not exist.                            | String  |
| `logs.max_count`                       | How many historic logs to keep                                                                        | Number  |
