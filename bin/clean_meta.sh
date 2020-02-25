#!/bin/bash

for arg in "$@"
do
    metadata-list -Q "{'\$and':[{'owner': 'mcleanj'},{'name': '$arg'}]}" | xargs -I % metadata-delete -V %
done