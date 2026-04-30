/*
The original code is from https://github.com/kubernetes/kubernetes/blob/master/pkg/kubectl/cmd/completion.go -

Copyright 2016 The Kubernetes Authors.
Modifications Copyright 2018 the Velero contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cmd

import (
	"bytes"
	"io"

	"github.com/spf13/cobra"
)

func GenZshCompletion(out io.Writer, velero *cobra.Command) {
	zshHead := "#compdef velero\n"

	out.Write([]byte(zshHead))

	zshInitialization := "\n__velero_bash_source() {\n\talias shopt=':'\n\talias _expand=_bash_expand\n\talias _complete=_bash_comp\n\temulate -L sh\n\tsetopt kshglob noshglob braceexpand\n\tsource \"$@\"\n}\n__velero_type() {\n\t# -t is not supported by zsh\n\tif [ \"$1\" == \"-t\" ]; then\n\t\tshift\n\t\t# fake Bash 4 to disable \"complete -o nospace\". Instead\n\t\t# \"compopt +-o nospace\" is used in the code to toggle trailing\n\t\t# spaces. We don't support that, but leave trailing spaces on\n\t\t# all the time\n\t\tif [ \"$1\" = \"__velero_compopt\" ]; then\n\t\t\techo builtin\n\t\t\treturn 0\n\t\tfi\n\ttype \"$@\"\n}\n__velero_compgen() {\n\tlocal completions w\n\tcompletions=( $(compgen \"$@\") ) || return $?\n\t# filter by given word as prefix\n\twhile [[ \"$1\" = -* && \"$1\" != -- ]]; do\n\t\tshift\n\t\tdone\n\tif [[ \"$1\" == -- ]]; then\n\t\tshift\n\tfi\n\tfor w in \"${completions[@]}\"; do\n\t\tif [[ \"${w}\" = \"$1\"* ]]; then\n\t\t\techo \"${w}\"\n\t\tfi\n\tdone\n}\n__velero_compopt() {\n\ttrue # don't do anything. Not supported by bashcompinit in zsh\n}\n__velero_ltrim_colon_completions()\n{\n\tif [[ \"$1\" == *:* && \"$COMP_WORDBREAKS\" == *:* ]]; then\n\t\t# Remove colon-word prefix from COMPREPLY items\n\t\tlocal colon_word=${1%${1##*:}}\n\t\tlocal i=${#COMPREPLY[*]}\n\t\twhile [[ $((--i)) -ge 0 ]]; do\n\t\t\tCOMPREPLY[$i]=${COMPREPLY[$i]#\"$colon_word\"}\n\t\tdone\n\tfi\n}\n__velero_get_comp_words_by_ref() {\n\tcur=\"${COMP_WORDS[COMP_CWORD]}\"\n\tprev=\"${COMP_WORDS[${COMP_CWORD}-1]}\"\n\twords=(\"${COMP_WORDS[@]}\")\n\tcword=(\"${COMP_CWORD[@]}\")\n}\n__velero_filedir() {\n\tlocal RET OLD_IFS w qw\n\t__velero_debug \"_filedir $@ cur=$cur\"\n\tif [[ \"$1\" = \\~* ]]; then\n\t\t# somehow does not work. Maybe, zsh does not call this at all\n\t\teval echo \"$1\"\n\t\treturn 0\n\tfi\n\tOLD_IFS=\"$IFS\"\n\tIFS=$'\\n'\n\tif [ \"$1\" = \"-d\" ]; then\n\t\tshift\n\t\tRET=( $(compgen -d) )\n\telse\n\t\tRET=( $(compgen -f) )\n\tfi\n\tIFS=\"$OLD_IFS\"\n\tIFS=\",\" __velero_debug \"RET=${RET[@]} len=${#RET[@]}\"\n\tfor w in ${RET[@]}; do\n\t\tif [[ ! \"${w}\" = \"${cur}\"* ]]; then\n\t\t\tcontinue\n\t\tfi\n\t\tif eval \"[[ \\\"\\${w}\\\" = *.$1 || -d \\\"\\${w}\\\" ]]\"; then\n\t\t\tqw=\"$(__velero_quote \"${w}\")\"\n\t\t\tif [ -d \"${w}\" ]; then\n\t\t\t\tCOMPREPLY+=(\"${qw}/\")\n\t\t\telse\n\t\t\t\tCOMPREPLY+=(\"${qw}\")\n\t\t\tfi\n\t\tdone\n}\n__velero_quote() {\n    if [[ $1 == \\'* || $1 == \\\"* ]]; then\n        # Leave out first character\n        printf %q \"${1:1}\"\n    else\n    \tprintf %q \"$1\"\n    fi\n}\nautoload -U +X bashcompinit && bashcompinit\n# use word boundary patterns for BSD or GNU sed\nLWORD='[[:<:]]'\nRWORD='[[:>:]]'\nif sed --help 2>&1 | grep -q GNU; then\n\tLWORD='\\<'\n\tRWORD='\\>'\nfi\n__velero_convert_bash_to_zsh() {\n\tsed \\\n\t-e 's/declare -F/whence -w/' \\\n\t-e 's/_get_comp_words_by_ref \"\\$@\"/_get_comp_words_by_ref \"\\$*\"/' \\\n\t-e 's/local \\([a-zA-Z0-9_]*\\)=/local \\1; \\1=/' \\\n\t-e 's/flags+=(\"\\(--.*\\)=\")/flags+=(\"\\1\"); two_word_flags+=(\"\\1\")/' \\\n\t-e 's/must_have_one_flag+=(\"\\(--.*\\)=\")/must_have_one_flag+=(\"\\1\")/' \\\n\t-e \"s/${LWORD}_filedir${RWORD}/__velero_filedir/g\" \\\n\t-e \"s/${LWORD}_get_comp_words_by_ref${RWORD}/__velero_get_comp_words_by_ref/g\" \\\n\t-e \"s/${LWORD}__ltrim_colon_completions${RWORD}/__velero_ltrim_colon_completions/g\" \\\n\t-e \"s/${LWORD}compgen${RWORD}/__velero_compgen/g\" \\\n\t-e \"s/${LWORD}compopt${RWORD}/__velero_compopt/g\" \\\n\t-e \"s/${LWORD}declare${RWORD}/builtin declare/g\" \\\n\t-e \"s/\\\\\\$(type${RWORD}/\\$(__velero_type/g\" \\"
	out.Write([]byte(zshInitialization))

	buf := new(bytes.Buffer)
	velero.GenBashCompletion(buf)
	out.Write(buf.Bytes())

	zshTail := `
BASH_COMPLETION_EOF
}
__velero_bash_source <(__velero_convert_bash_to_zsh)
_complete velero 2>/dev/null
`
	out.Write([]byte(zshTail))
}
