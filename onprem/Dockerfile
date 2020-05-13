FROM mcr.microsoft.com/dotnet/core/sdk:3.1 as build
RUN apt-get update
RUN apt-get install -y git
RUN git clone --recursive https://github.com/mspnp/azure-databricks-streaming-analytics.git  &&  cd azure-databricks-streaming-analytics && git fetch && git checkout master
WORKDIR azure-databricks-streaming-analytics/onprem/DataLoader
RUN dotnet build
RUN dotnet publish -f netcoreapp3.1 -c Release
FROM mcr.microsoft.com/dotnet/core/runtime:3.1 AS runtime
WORKDIR DataLoader
COPY --from=build azure-databricks-streaming-analytics/onprem/DataLoader/bin/Release/netcoreapp3.1/publish .
ENTRYPOINT ["dotnet" , "taxi.dll"]
