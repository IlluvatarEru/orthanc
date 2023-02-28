#!/bin/bash
shopt -s expand_aliases
. scripts/utils.bash
a=$(get_python_alias)
alias py=$a
py -m launch.kz.check_bi Astana Aqua 1;
#py -m launch.kz.check_bi Astana Nexpo 1;
py -m launch.kz.check_bi Astana Tokyo 1;
py -m launch.kz.check_bi Astana Flora 1;