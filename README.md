# Tea Parser

## Docker setup

All Docker-related files are now located in the `docker/` folder. To start the
development environment you need Docker and Docker Compose installed. Run the
following command from the repository root:

```bash
docker compose -f docker/docker-compose.yml up --build
```

This will build the backend and frontend images and launch the MongoDB,
backend and frontend containers. You can also `cd docker` and run `docker
compose up --build` if you prefer.
