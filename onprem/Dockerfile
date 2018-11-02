FROM microsoft/dotnet:2.1-sdk AS build
RUN apt-get update
RUN apt-get install -y git
RUN git clone --recursive https://github.com/mspnp/reference-architectures.git  &&  cd reference-architectures && git fetch && git checkout master
WORKDIR reference-architectures/data/streaming_asa/onprem/DataLoader
RUN dotnet build
RUN dotnet publish -f netcoreapp2.0 -c Release


FROM microsoft/dotnet:2.1-runtime AS runtime
WORKDIR DataLoader
COPY --from=build reference-architectures/data/streaming_asa/onprem/DataLoader/bin/Release/netcoreapp2.0/publish .
ENTRYPOINT ["dotnet" , "taxi.dll"]
