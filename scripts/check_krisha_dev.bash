#!/bin/bash
shopt -s expand_aliases
. scripts/utils.bash
a=$(get_python_alias)
alias py=$a
py -m launch.kz.check_krisha Astana Headliner 1 DEV;
#py -m launch.kz.check_krisha Astana Aqua 1 DEV;
#py -m launch.kz.check_krisha Astana nexpo 1 DEV;
py -m launch.kz.check_krisha Almaty Jazz 2 DEV;
#py -m launch.kz.check_krisha Astana Flora 1 DEV;
#py -m launch.kz.check_krisha Astana Tokyo 1 DEV;
