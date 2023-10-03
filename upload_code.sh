#!/bin/bash

rsync -avz snapper.py utils.py reports/*.py config.schema.json requirements.txt nas:/torrent/tools/snapper

