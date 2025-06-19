#!/bin/bash
set -e

superset db upgrade
superset fab create-admin \
    --username ${ADMIN_USERNAME:-admin} \
    --firstname Admin \
    --lastname User \
    --email ${ADMIN_EMAIL:-mutanterfmika@gmail.com} \
    --password ${ADMIN_PASSWORD:-admin}
superset init
