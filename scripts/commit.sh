#!/bin/sh
# bash script to commit in accordance with the branch name
# usage: bash scripts/commit.sh
raw_branch=$(git branch | sed -n -e 's/^\* \(.*\)/\1/p')
branch="${raw_branch##*fix/}"
branch="${branch##*feat/}"
branch="${branch//\_/ }"
issue_number="${branch//issue\-/}"
title=$( cut -d ' ' -f 2- <<< "$issue_number" )
issue_number=$( cut -d ' ' -f 1 <<< "$issue_number" )
if [[ ${raw_branch,} =~ "fix" ]]; then
  commit_message="fix: $title - closes #$issue_number"
else
  commit_message="feat: $title - closes #$issue_number"
fi

git commit -sam "$commit_message"