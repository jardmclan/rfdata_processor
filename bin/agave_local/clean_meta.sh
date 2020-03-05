#!/bin/bash

for arg in "$@"
do
    metadata-list -l 0 -Q "{'\$and':[{'owner': 'mcleanj'},{'name': '$arg'}]}" | xargs -I % metadata-delete %
done