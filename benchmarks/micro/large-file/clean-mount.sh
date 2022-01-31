if [ $# -gt 1 ]; then
    echo "Invalid argument!"
    exit 1
fi

../../cleanup-fs.sh
../../setup-ftfs.sh $1
