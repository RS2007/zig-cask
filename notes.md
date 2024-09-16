## Bitcask
* Bitcask instance is a directory. 
* only one process can open a bitcask instance at a time.
* One file in the directory is active(Only open file, will be closed after reaching the threshold)
* Once a file is closed, either purposefully or due to server exit, it is considered immutable and will never be opened for writing again.
*
