# cards-server

Lobby and move relay for the Sailfish OS card games
[Bridge](https://github.com/klahr/bridge), [Cribbage](https://github.com/klahr/cribbage)
and [Hearts](https://github.com/klahr/hearts). The rules run in the apps; the server
only helps players find, join and rejoin tables, and keeps each table's log of moves.

```
go build
./cards-server -addr :7777
```

Or with Docker:

```
docker build -t cards-server .
docker run -d -p 7777:7777 cards-server
```

## License

AGPLv3, see [LICENSE](LICENSE).
