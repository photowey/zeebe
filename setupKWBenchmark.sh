#!/bin/bash
set -exuo pipefail

mvn clean install -DskipTests -T1C

cw=$(date +%V)
commitHash=$(git rev-parse --short HEAD)
benchmark="medic-cw-$cw-$commitHash-benchmark"

docker build --build-arg DISTBALL=dist/target/zeebe-distribution-*.tar.gz --build-arg APP_ENV=dev -t "gcr.io/zeebe-io/zeebe:$benchmark" .
docker push "gcr.io/zeebe-io/zeebe:$benchmark"

cd benchmarks/setup/

./newBenchmark.sh "$benchmark"

cd "$benchmark"

sed -i 's/camunda\/zeebe/gcr.io\/zeebe-io\/zeebe/' zeebe-values.yaml
sed -i "s/SNAPSHOT/$benchmark/" zeebe-values.yaml

make zeebe starter worker

git add .
git commit -m "add $benchmark"
git push origin medic-cw-benchmarks
