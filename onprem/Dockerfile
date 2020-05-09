FROM mcr.microsoft.com/dotnet/core/sdk:3.1 as build
RUN apt-get update
RUN apt-get install -y git
RUN git clone --recursive https://github.com/mspnp/reference-architectures.git  &&  cd reference-architectures && git fetch && git checkout bugfix/no-warnings
WORKDIR reference-architectures/data/streaming_asa/onprem/DataLoader
RUN dotnet build
RUN dotnet publish -f netcoreapp3.1 -c Release

FROM mcr.microsoft.com/dotnet/core/runtime:3.1 AS runtime
WORKDIR DataLoader
COPY --from=build reference-architectures/data/streaming_asa/onprem/DataLoader/bin/Release/netcoreapp2.0/publish .
ENTRYPOINT ["dotnet" , "taxi.dll"]
