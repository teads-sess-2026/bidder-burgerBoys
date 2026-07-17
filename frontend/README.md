# SESS-Frontend

A starter dashboard that connects to a bidder's **Stats API** and displays its performance
(overview KPIs, per-creative breakdown, targeting results, and recent trends). Fork this repo
and point it at your own bidder to build on top of it.

Set the bidder URL (`backendBaseUrl`) in the environment files — see [Set backend URL](#set-backend-url).
The API is documented in [`STATS_API.md`](https://github.com/teads-sess-2026/bidder/blob/main/STATS_API.md).

## Prerequisites
- npm v11+
- Node v22.22.3+ (or v24.15+ / v26+) — required by Angular 22

## Development server
Run `ng serve` to start a development server.
- The application will run at `http://localhost:4200/`.
- When running on the development server, the application is connected to the **local development backend**
- If you want the local development server to connect to the **production backend**, start it with the command `ng serve -c production`
- The application will automatically reload if you change any of the source files.
- You can stop the development server by pressing `Ctrl + C` in the console

## Set backend URL
You can change the location of the backend in the environment files:
- `src/environments/environment.development.ts` for **local development backend**
- `src/environments/environment.ts` for **production backend**

## Build
Run `ng build` to build the project. The build artifacts will be stored in the `dist/` directory.

## Running in Docker
If you wish to run the application in Docker (which is optional), use the following commands:
- `make build_docker`: Prepares a Docker image
- `make run_docker`: Runs the docker image and serves the application on port 80.
  - It can be accessed at the URL http://localhost
  - When running in Docker, the application is connected to the **production backend**
- `make stop_docker`: Stops the Docker instance

## Forking the project
If you want to use this project as a basis for your own FE application, you should do the following:
1. Fork this repository in GitHub:
![252868690-15bee300-c7d5-4c2c-a878-c262df343664](https://github.com/ob-fsss-2024/frontend/assets/36840705/31571571-bebe-457c-a3a5-843289b2f0f7)
2. Ask somebody with access to the Azure portal to create a deployment for you (Preferably @jgosar). This is what they will need to do:
- Go to https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Web%2FStaticSites
- Click Create
- Basic tab:
  - Subscription:
    - Resource Group: teads-sess-2025
  - Static Web App details:
    - Name: Name of the web app
  - Deployment details:
    - Source: Other
- Advanced tab:
  - Region: West Europe
- Click Review + create
- Go back to https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Web%2FStaticSites and wait for the new app to appear (~1 minute)
- Click on the new app, open the tab "Manage deployment token" and copy the value
- In the GitHub project set the following:
  - Settings -> Secrets and variables -> Actions -> New repository secret
    - Name: AZURE_TOKEN
    - Value: [paste token value]
3. In the GitHub project go to the Actions tab:
  - Click on "I understand my workflows, go ahead and enable them"
  - Go to the action "Azure Static Web Apps CI/CD" in the sidebar
  - Click "Run workflow"
  - Open the build status by clicking on the "Azure Static Web Apps CI/CD" entry that appeared below
 ![image](https://github.com/ob-fsss-2024/frontend/assets/36840705/c69c822f-8f3e-448d-803d-1d691e63c819)
4. Open "Build and Deploy job" and wait for the "Build and Deploy" phase to complete
5. Look for this in the output logs: `Visit your site at: https://[some-unique-url].azurestaticapps.net`
6. Your app should be running on the specified URL, connected to the **production backend**

