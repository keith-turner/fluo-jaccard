MAX=$1
shift

for i in $(seq 0 $(($MAX - 1))); do
  $FLUO_HOME/bin/fluo exec jaccard fj.cmd.Load $i $MAX "$@"
  $FLUO_HOME/bin/fluo wait jaccard
done

