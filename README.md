# IqFeedSvc

Flask app that exposes an api at port `8080` for starting workflows to pull historical price data from IQFeed. Also has scheduled workflows to get price history.

**Check main repo for an explanation of the schedule already set for retrieving data automatically.**

[Main Repo](https://github.com/faquino08/FinanceDb/blob/main/README.md)

# Docker Reference

The following is a description of each env var key and value:

**Key Name:** PROJECT_ROOT \
**Description:** :warning: DEPRECATED :warning: a string containing the authentication information for the postgres server. DO NOT INCLUDE DATABASE NAME. \
**Values:** <span style="color:#6C8EEF">user:password@host:port</span>

**Key Name:** POSTGRES_DB \
**Description:** a string containing the name of the postgres database for data insertion. \
**Values:** <span style="color:#6C8EEF">\<postgres database name string></span>

**Key Name:** POSTGRES_USER \
**Description:**  a string containing the username the postgres server to use for authentication. \
**Values:** <span style="color:#6C8EEF">\<postgres username string></span>

**Key Name:** POSTGRES_PASSWORD \
**Description:** a string containing the password the postgres user specified. \
**Values:** <span style="color:#6C8EEF">\<postgres password string></span>

**Key Name:** POSTGRES_LOCATION \
**Description:** a string containing the hostname for the postgres server. \
**Values:** <span style="color:#6C8EEF">\<postgres hostname string></span>

**Key Name:** POSTGRES_PORT \
**Description:** a string containing the port for the postgres server. \
**Values:** <span style="color:#6C8EEF">\<postgres port string></span>

**Key Name:** IQ_LOCATION \
**Description:** a string containing the hostname for the iqfeed client. \
**Values:** <span style="color:#6C8EEF">\<IqFeed hostname string></span>

**Key Name:** IQ_PORT \
**Description:** a string containing the port to reach the iqfeed client. \
**Values:** <span style="color:#6C8EEF">\<IqFeed port string></span>

**Key Name:** DEBUG_BOOL \
**Description:** a string determining whether logging should include debug level messages. \
**Values:** <span style="color:#6C8EEF">True|False</span>

**Key Name:** DEFAULT_MIN_START \
**Description:** a string for the earliest date of minute level price history to get data for if not specified during call of workflow. \
**Values:** <span style="color:#6C8EEF">YYYY-MM-dd HH:MM:SS|null</span>

**Key Name:** DEFAULT_DAILY_START \
**Description:** a string for the earliest date of daily level price history to get data for if not specified during call of workflow. \
**Values:** <span style="color:#6C8EEF">YYYY-MM-dd|null</span>

# Api Reference

[comment]: <> (First Command)
### <span style="color:#6C8EEF">**POST**</span> /run_pricehist?delay=<span style="color:#a29bfe">**:int**</span>&minStart=<span style="color:#a29bfe">**:str**</span>&dayStart=<span style="color:#a29bfe">**:str**</span>&fullMarket=<span style="color:#a29bfe">**:boolean**</span>

#### **Arguments:**
- **delay** - integer showing how many seconds before starting the workflow. *Default:* ***10***
- **minStart** - string for the earliest datetime of price history to get in format *YYYY-MM-dd HH:MM:SS*. \
*Default:* **DEFAULT_MIN_START or '20200101 073000' depending on deployment**
- **dayStart** - string for the earliest date of price history to get in format *YYYY-MM-dd*. \
*Default:* **DEFAULT_DAILY_START or '20180101' depending on deployment**
- **fullMarket** - should the price history be retrieved for all listed equities or just the ones that are in major indices/optionable? *Default:* ***False***
