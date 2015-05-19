#!/bin/bash
for f in output/*.dot
do
 echo "Rendering: $f"
 dot -Tsvg $f -o $f.svg &
done
