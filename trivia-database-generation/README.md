# Trivia Game Database Generation

## Purpose
This code provides a one-time setup for the database used in the trivia game frontend.

## Setup
- Create a .env.development and .env.production file mirror the provided schema with your private keys
### Running Development Server
- Run `$ make serve` to run the development server
### Loading the database
- Edit load_database.py to set IS_DEVELOPMENT to True/False depending on destination environment.
- Run `# make setup` to setup python virtual environment
- Run `# make load` to load the data into selected environment

## Notes:
When running directly to neon/production, this code will take a long time (10+ hours). It is therefore recommended to create the development postgres db using docker, then making a dump and pushing that to neon using the [neon migration tool](https://neon.com/docs/import/migrate-from-postgres).
