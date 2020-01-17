#!/bin/sh

beeline -u 'jdbc:hive2://localhost:10000 project project' -f CreateDatabase.sql
