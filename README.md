# Pop2Exchange-Exporter

Log file parser and exporter to the prometheus monitoring system for the [Pop2Exchange Connector](http://www.roth-web.com/).

- parse log file (path currently hard coded to `/media/Pop2Exchange.log`)
- insert entries from the log file into an SQLite database
- extract important properties from SQLite database and export them via a http interface

## Todo

- add tests
- add ways to configure the package
- add integrity checks
- make readme more meaningful

## License

GNU Affero General Public License

https://www.gnu.org/licenses/agpl-3.0.de.html
