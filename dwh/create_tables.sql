create database dwh;
\c dwh;

create schema if not exists sta_github;
create schema if not exists stg_github;

DROP TABLE IF EXISTS sta_github.issues;
CREATE TABLE IF NOT EXISTS  sta_github.issues(
        "number" int NOT NULL,
        id bigint  NOT NULL,
        title varchar NOT NULL,
        repo_name varchar NOT NULL,
        pull_request json,
        milestone json,
        labels json,
        pull_request_number bigint,
        pull_request_merged_at timestamp,
        created_at timestamp NOT NULL,
        "state" varchar NOT NULL,
        state_reason varchar,
        closed_at timestamp,
        updated_at timestamp,
        integrated_at DATE NOT NULL DEFAULT CURRENT_DATE ,
        CONSTRAINT PK_issues PRIMARY KEY ( "repo_name","number","integrated_at"),
  CONSTRAINT UK_issues_id UNIQUE ( "repo_name", "number" ,"integrated_at" )
)PARTITION BY range (integrated_at);

DROP TABLE IF EXISTS sta_github.pull_requests;
CREATE TABLE IF NOT EXISTS sta_github.pull_requests(
        id bigint NOT NULL,
        "number" int NOT NULL,
        repo_name varchar,
        "state" varchar NOT NULL,
        title varchar NOT NULL,
        body varchar,
        created_at timestamp NOT NULL,
        updated_at timestamp ,
        closed_at timestamp ,
        merged_at timestamp ,
        labels json,
        milestone_number bigint,
        milestone_title varchar,
        draft boolean,
        merged boolean,
        mergeable_state varchar,
        additions int,
        deletions int,
        linked_issue_number_in_comments bigint,
        integrated_at DATE NOT NULL DEFAULT CURRENT_DATE  ,
        CONSTRAINT PK_pull_requests PRIMARY KEY ( "repo_name","number","integrated_at"),
  CONSTRAINT UK_pull_requests_id UNIQUE ( "repo_name", "number" ,"integrated_at" )
)PARTITION BY range (integrated_at);

DROP TABLE IF EXISTS sta_github.pull_requests_labels;
CREATE TABLE IF NOT EXISTS sta_github.pull_requests_labels(
        pull_requests_id BIGINT NOT NULL,
        pull_request_number int NOT NULL,
        repo_name varchar NOT NULL,
        label_id varchar NOT NULL,
        label_name varchar NOT NULL,
        label_default boolean,
        integrated_at DATE NOT NULL DEFAULT CURRENT_DATE
)PARTITION BY range (integrated_at);

DROP TABLE IF EXISTS stg_github.labels_metrics;
CREATE TABLE IF NOT EXISTS stg_github.labels_metrics(
        label_id varchar NOT NULL PRIMARY KEY,
        label_name varchar NOT NULL,
        nb_pull_requests bigint,
        integrated_at DATE NOT NULL DEFAULT CURRENT_DATE
);


DROP TABLE IF EXISTS stg_github.pull_requests_metrics;
CREATE TABLE IF NOT EXISTS stg_github.pull_requests_metrics (
        pull_requests_id BIGINT NOT NULL PRIMARY KEY,
        pull_request_title TEXT,
        pull_request_created_at TIMESTAMP(6) WITHOUT TIME ZONE,
        issue_id BIGINT,
        issue_number INTEGER,
        pull_request_number INTEGER,
        issue_created_at TIMESTAMP(6) WITHOUT TIME ZONE,
        pull_request_merged_at TIMESTAMP(6) WITHOUT TIME ZONE,
        pull_request_state TEXT,
        issue_state TEXT,
        milestone_number BIGINT,
        milestone_title TEXT,
        lines_additions INTEGER,
        lines_deletions INTEGER,
        total_lines_changed INTEGER,
        pull_request_lead_time_mins DOUBLE PRECISION ,
        merge_interval DOUBLE PRECISION,
        updated_at TIMESTAMP
);

CREATE TABLE if not exists sta_github.issues_20230625
PARTITION OF sta_github.issues FOR VALUES FROM ('2023-06-25') TO ('2023-06-26');

CREATE TABLE if not exists sta_github.issues_20230626
PARTITION OF sta_github.issues  FOR VALUES FROM ('2023-06-26') TO ('2023-06-27');

CREATE TABLE if not exists sta_github.issues_20230627
PARTITION OF sta_github.issues FOR VALUES FROM ('2023-06-27') TO ('2023-06-28');

CREATE TABLE if not exists sta_github.issues_20230628
PARTITION OF sta_github.issues  FOR VALUES FROM ('2023-06-28') TO ('2023-06-29');

CREATE TABLE if not exists sta_github.issues_20230629
PARTITION OF sta_github.issues  FOR VALUES FROM ('2023-06-29') TO ('2023-06-30');

CREATE TABLE if not exists sta_github.issues_20230630
PARTITION OF sta_github.issues FOR VALUES FROM ('2023-06-30') TO ('2023-07-01');

CREATE TABLE if not exists sta_github.pull_requests_20230626
PARTITION OF sta_github.pull_requests  FOR VALUES FROM ('2023-06-26') TO ('2023-06-27');

CREATE TABLE if not exists sta_github.pull_requests_20230627
PARTITION OF sta_github.pull_requests FOR VALUES FROM ('2023-06-27') TO ('2023-06-28');

CREATE TABLE if not exists sta_github.pull_requests_20230628
PARTITION OF sta_github.pull_requests  FOR VALUES FROM ('2023-06-28') TO ('2023-06-29');

CREATE TABLE if not exists sta_github.pull_requests_20230629
PARTITION OF sta_github.pull_requests  FOR VALUES FROM ('2023-06-29') TO ('2023-06-30');

CREATE TABLE if not exists sta_github.pull_requests_20230630
PARTITION OF sta_github.pull_requests FOR VALUES FROM ('2023-06-30') TO ('2023-07-01');

CREATE TABLE if not exists sta_github.pull_requests_labels_20230626
PARTITION OF sta_github.pull_requests_labels  FOR VALUES FROM ('2023-06-26') TO ('2023-06-27');

CREATE TABLE if not exists sta_github.pull_requests_labels_20230627
PARTITION OF sta_github.pull_requests_labels FOR VALUES FROM ('2023-06-27') TO ('2023-06-28');

CREATE TABLE if not exists sta_github.pull_requests_labels_20230628
PARTITION OF sta_github.pull_requests_labels  FOR VALUES FROM ('2023-06-28') TO ('2023-06-29');

CREATE TABLE if not exists sta_github.pull_requests_labels_20230629
PARTITION OF sta_github.pull_requests_labels  FOR VALUES FROM ('2023-06-29') TO ('2023-06-30');

CREATE TABLE if not exists sta_github.pull_requests_labels_20230630
PARTITION OF sta_github.pull_requests_labels FOR VALUES FROM ('2023-06-30') TO ('2023-07-01');


