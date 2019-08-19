CREATE DATABASE IF NOT EXISTS ${hivevar:env}_${hivevar:org}_${hivevar:dvr}_inb LOCATION '/${hivevar:env}/${hivevar:org}/data/${hivevar:ara}/${hivevar:dvr}/inb';
CREATE DATABASE IF NOT EXISTS ${hivevar:env}_${hivevar:org}_${hivevar:dvr}_stg LOCATION '/${hivevar:env}/${hivevar:org}/data/${hivevar:ara}/${hivevar:dvr}/stg';
CREATE DATABASE IF NOT EXISTS ${hivevar:env}_${hivevar:org}_${hivevar:dvr}_wrk LOCATION '/${hivevar:env}/${hivevar:org}/data/${hivevar:ara}/${hivevar:dvr}/wrk';
CREATE DATABASE IF NOT EXISTS ${hivevar:env}_${hivevar:org}_${hivevar:dvr}_ref LOCATION '/${hivevar:env}/${hivevar:org}/data/${hivevar:ara}/${hivevar:dvr}/ref';
CREATE DATABASE IF NOT EXISTS ${hivevar:env}_${hivevar:org}_${hivevar:dvr}_whs LOCATION '/${hivevar:env}/${hivevar:org}/data/${hivevar:ara}/${hivevar:dvr}/whs';
CREATE DATABASE IF NOT EXISTS ${hivevar:env}_${hivevar:org}_${hivevar:dvr}_oub LOCATION '/${hivevar:env}/${hivevar:org}/data/${hivevar:ara}/${hivevar:dvr}/oub';
CREATE DATABASE IF NOT EXISTS ${hivevar:env}_${hivevar:org}_${hivevar:dvr}_arh LOCATION '/${hivevar:env}/${hivevar:org}/data/${hivevar:ara}/${hivevar:dvr}/arh';
CREATE DATABASE IF NOT EXISTS ${hivevar:env}_${hivevar:org}_${hivevar:dvr}_flr LOCATION '/${hivevar:env}/${hivevar:org}/data/${hivevar:ara}/${hivevar:dvr}/flr';

