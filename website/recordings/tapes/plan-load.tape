Source tapes/common.tape
Output out/plan-load.gif
Output out/plan-load.mp4
Hide
Type "cd scratch/plan-load/shop && unset NO_COLOR && export PS1='$ ' && clear"
Enter
Sleep 500ms
Show
Type "sqlload plan"
Sleep 300ms
Enter
Sleep 3.5s
Hide
Type "clear"
Enter
Sleep 200ms
Show
Type "sqlload load"
Sleep 300ms
Enter
Sleep 3.5s
Hide
Type "cp ../typo.toml sqlload.toml && clear"
Enter
Sleep 200ms
Show
Type "grep cursor_column sqlload.toml"
Sleep 200ms
Enter
Sleep 800ms
Type "sqlload load"
Sleep 300ms
Enter
Sleep 3s
Screenshot out/plan-load.png
Sleep 1s
