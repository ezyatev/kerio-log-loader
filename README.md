# Kerio Log Loader

The script loads Kerio HTTP log into MariaDB/MySQL using multiprocessing library to do it efficiently.
It can help you to save full business days in data loading and get immediate, valuable insight into your data.

Requires Python 3.7+ and pipenv.

## HTTP log example

HTTP log has the standard format of either the Apache WWW server (see http://www.apache.org/) or of the Squid proxy server (see http://www.squid-cache.org/).

An example of an HTTP log record in the Apache format:

```
192.168.12.6 - - [21/Mar/2019:14:22:34 +0300] "GET http://example.com/media/sidebar.jpg HTTP/1.1" 200 31378 +2
192.168.12.6 - - [21/Mar/2019:14:22:34 +0300] "GET http://example.com/media/banner1.jpg HTTP/1.1" 200 28716 +3
192.168.12.6 - - [21/Mar/2019:14:22:34 +0300] "GET http://example.com/media/banner2.jpg HTTP/1.1" 200 14080 +4
192.168.12.6 - - [21/Mar/2019:14:22:35 +0300] "GET http://example.com/static/bundle.min.css HTTP/1.1" 200 227528 
...
```

## Key Features

1. Increase granularity. Splitting the file into big chunks will introduce a data skew. 
2. Multi-inserts. The script use batch inserts into 100 row multi-inserts. This will reduce the number of network roundtrips. 
3. Load in parallel. 

## Installation

1. Clone project repo.

2. Create database user and grant privileges:

    ```
    $ mysql -u root -p
    > GRANT ALL PRIVILEGES ON `log`.* TO 'log'@'localhost' IDENTIFIED BY 'YOUR_PASS';
    ```

3. Create database:

    ```
    $ mysql -u log -p
    > CREATE DATABASE log;
    > USE log;
    > CREATE TABLE log (
        id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
        ip VARCHAR(15) NOT NULL, 
        dt DATETIME NOT NULL, 
        url VARCHAR(500), 
        CONSTRAINT pk_log PRIMARY KEY (id));
    > CREATE INDEX ip_idx ON log(ip);
    > CREATE INDEX dt_idx ON log(dt);
    > CREATE INDEX url_idx ON log(url);
    ```

4. Install dependencies:

    ```
    $ pipenv install

    ```

5. Copy the `.env-example` file, edit and save as `.env` in the same directory.


## Usage

Run in your shell:

```
$ pipenv shell
$ python load.py -f http.log
```

Enjoy :-)

## Author

Eugene Zyatev (eu@zyatev.ru)
