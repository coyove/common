# logg

Basic usage: 
`var logger = logg.NewLogger(<FORMAT>)`

Where `<FORMAT>` is a string whose content, for example, can be:

1. `dbg0`: output `DEBUG0`, `DEBUG`, `LOG`, `INFO`, `WARNING`, `ERROR`, `FATAL`;
2. `log`: output `LOG`, `INFO`, `WARNING`, `ERROR`, `FATAL`;
2. `warn`: output `WARNING`, `ERROR`, `FATAL`;
2. `fatal`: output `FATAL` only;
2. `off`: disable the output;
2. `log^info^warn`: output `LOG`, `ERROR`, `FATAL` with `INFO` and `WARN` being ignored;
2. `log:pathtofile.csv`: output to a file;
2. `log:512000+pathtofile.csv`: output to a file, split every 512000 bytes;
