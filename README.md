# Get Early CloudSQL GCP Maintenance Notification

This tools is for support operational work to get early notification for maintenance cloudsql from google cloud platform. This tools using python

## Tech Stack

![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)
![Python](https://img.shields.io/badge/python-3.12-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Shell Script](https://img.shields.io/badge/shell_script-%23121011.svg?style=for-the-badge&logo=gnu-bash&logoColor=white)
![PDM](https://img.shields.io/badge/PDM-%233B82F6.svg?style=for-the-badge&logo=pdm&logoColor=0B3D8D&color=AC75D7)
![.ENV](https://img.shields.io/badge/env-%233B82F6.svg?style=for-the-badge&logo=.env&logoColor=0B3D8D&color=ECD53F)

### Set Environment Value For Token Opsgenie

copy file `env.example` to `.env`

```bash
cp env.example .env
```

## Copy Service Account File to dedicated directory

```bash
cp <your_service_account>.json src/backup_engine/service_account/<your_service_account>.json
```

## Build Docker Image

```bash
docker image build -t backup_engine:1.0 .
```

## Deploy To Container

```bash
docker compose up -d
```
