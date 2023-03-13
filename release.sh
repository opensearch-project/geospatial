#!/bin/bash

pr=$(gh pr list --label v2.4.0 --state merged --json number,title,url,baseRefName)

for row in $(echo "${pr}" | jq -r '.[] | @base64'); do
    _jq() {
     echo ${row} | base64 --decode | jq -r ${1}
    }
   title=$(_jq '.title') 
   trimed_title=$(echo $title | sed -E 's/\[.*\] *//g' | sed -E 's/\(#[0-9]+\)//g')
   number=$(_jq '.number')
   url=$(_jq '.url')
   base=$(_jq '.baseRefName')
   echo "* ${trimed_title} ([#${number}](${url})) [${base}]"
done
