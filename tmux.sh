#!/bin/bash
tmux new-session -d -s toku -n main
tmux new-window -t toku:1 -n grep
tmux new-window -t toku:2 -n compile
tmux new-window -t toku:3 -n debug
tmux new-window -t toku:4 -n paper
tmux select-window -t toku:0