#!/bin/sh
set -e
cue export --out yaml test.cue > test.yml
git wip
git push
