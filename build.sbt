get_property() {
    key=$1
    value=$(grep -m 1 "^$key[[:space:]]*=" "$PROPERTIES_FILE" | cut -d'=' -f2- | tr -d '[:space:]')
    if [ -z "$value" ]; then
        echo "Error: Key '$key' not found in $PROPERTIES_FILE" >&2
    fi
    echo "$value"
}
