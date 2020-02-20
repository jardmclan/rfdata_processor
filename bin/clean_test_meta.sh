#!/bin/bash

metadata-list -Q "{'\$and':[{'owner': 'mcleanj'},{'name': 'test'}]}" | xargs -I % metadata-delete -V %