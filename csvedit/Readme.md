# csvedit

A util that parses csv data and provides some basic editing. Because no one likes complicated regexes to deal with escaped commas when you have csvs with untrusted data.

## Operations
* `grep foo bar` filters for rows where the column `foo` contains the substring `bar`
* `grepv foo bar` filters for rows where the column `foo` does not contain the substring `bar`
* `cut foo` removes all other columns, and returns column `foo`
* `shuffle foo` moves columns `foo` (and the others to the back) of the row 
* `sort foo` stable sorts the rows by `foo` lexically
* `sorti foo` stable sorts the rows by `foo` as integers
* `sortf foo` stable sorts the rows by `foo` as floats

Multiple columns can be provided to `cut`, `shuffle`, `sort`, `sorti`, `sortf` by separating the names with a comma.

If multiple columns are provided to a `sort`, then the first column will always be sorted, and for tied values on the first column it will use the second column, and so on.

Example usage:
```
% cat samples/abc.csv | csvedit grepv B e
A,B,C
a,b,c
1,2,3
uno,dos,tres
% cat samples/abc.csv | csvedit grepv B e shuffle B,A
B,A,C
b,a,c
2,1,3
dos,uno,tres
% cat samples/abc.csv | csvedit grepv B e cut B,A    
B,A
b,a
2,1
dos,uno
% cat samples/nums.csv | csvedit sort English
Numeric,English
5,five
4,four
1,one
7,seven
77,seventy seven
6,six
66,sixty six
3,three
12,twelve
2,two
% cat samples/nums.csv | csvedit sorti Numeric
Numeric,English
1,one
2,two
3,three
4,four
5,five
6,six
7,seven
12,twelve
66,sixty six
77,seventy seven
```
