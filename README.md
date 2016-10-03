Transtats is a flywaydb project to import database from [Bureau of transportation statistics](http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time)

Installation
=============

Clone this repository in your [postgresql virtual machine](https://github.com/Esme-Sudria-Database/Vagrant-Postgresql)

    vagrant ssh

    git clone https://github.com/Esme-Sudria-Database/Flywaydb-Transtats.git

Execute the migration

    cd Flywaydb-Transtats
    mvn flyway:migrate

You can clean the database by executing

    mvn flyway:clean

To execute more migration, modify version target into pom.xml
from 20150925_1900 to 20150925_2000 :

    <build>
    <plugins>
      <plugin>
        ...
        <configuration>
          ...
          <target>20150925_1900</target>
        </configuration>
      </plugin>
    </plugins>
  </build>


Main requests
==============

Get all unique origin airport and cities
-----------------------------------------

        SELECT origin_airport_id, origin_city_name, count(*) as count_travel FROM transtats GROUP BY origin_airport_id, origin_city_name ORDER BY count_travel DESC;

Select data from origin after migration
-------------------------------------------

        SELECT * FROM transtats t
         INNER JOIN transtats_city_origin tco ON t.origin_airport_id=tco.origin_airport_id;
