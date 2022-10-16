#!/bin/bash

get_python_alias() {
  unameOut="$(uname -s)"
  case "${unameOut}" in
      Linux*)     machine="Linux";;
      Darwin*)    machine="Mac";;
      CYGWIN*)    machine="Cygwin";;
      MINGW*)     machine="MinGw";;
      *)          machine="UNKNOWN:${unameOut}"
  esac
  if [[ $machine == "Linux" ]]; then
    echo "Running on Linux"
    echo  'python3' #-m launch.launch_mtm_aeglos True;
  else
    echo "Running on Windows"
    echo  'C:/Users/arthu/anaconda3/python.exe' # -m launch.launch_mtm_aeglos True
  fi
}