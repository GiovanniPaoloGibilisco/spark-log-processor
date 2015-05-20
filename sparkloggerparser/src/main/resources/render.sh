#!/bin/bash
for f in output/*/*.dot
do
 echo "Rendering: $f"
 dot -Tpng $f -o $f.png &
done
