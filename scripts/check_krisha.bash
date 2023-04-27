#!/bin/bash
shopt -s expand_aliases
. scripts/utils.bash
a=$(get_python_alias)
alias py=$a
py -m launch.kz.check_krisha Astana Headliner 2 PROD;
#py -m launch.kz.check_krisha Astana Aqua 1 PROD;
#py -m launch.kz.check_krisha Astana nexpo 1 PROD;
py -m launch.kz.check_krisha Almaty Jazz 2 PROD;
#py -m launch.kz.check_krisha Astana Flora 1 PROD;
#py -m launch.kz.check_krisha Astana Tokyo 1 PROD;