# Define colors
GREEN="\[\e[32m\]"
BLUE="\[\e[34m\]"
YELLOW="\[\e[33m\]"
MAGENTA="\[\e[35m\]"
RED="\[\e[31m\]"
RESET="\[\e[0m\]"

# Set up the prompt
export PS1="${GREEN}\u${RESET}${RED}@${BLUE}\h${RESET}:${YELLOW}\w${RESET}$  "

