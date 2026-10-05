** Project Setup **

1. Install gcloud
2. Initialize gcloud
3. Create a new project id and select that project id(in gcloud init command)
4. create 3 buckets in this project id(one for receiving new file, one for storing processed files in archive and another for storing DQ failed records.)

--File Description--\n
sr. no. - unique id for each car sale transaction - STRING\n
car_name - Name of the car - STRING\n
brand - Brand name of the car - STRING\n
model - model name of the car - STRING\n
vehicle_age - no. of years post manfgd date - INT\n
km_driven - no. of Kilometers driven - INT\n
seller_type -  individual or dealer -STRING\n
fuel_type -  Type of fuel supported by the car  - STRING\n
transmission_type - Manual/Automatic -STRING\n
mileage - no. of KMs covered per ltr of fuel - DOUBLE/FLOAT\n
engine - engine capacity(in cc) of the CAR - INT\n
max_power - MAX power of the car - DOUBLE/FLOAT\n
seats - no. of seats in the car  - INT\n
selling price - final price of the sale - INT\n

--Cloud Functions Functionality--
1. It should be written in pyspark
2. It should quarantine records failing DQ checks instead of the entire file.Keep Pydantic based DQ validation(if data type is not matching then entire file should be quarantined)
3. If records are quarantined, then add an additional column called file_name in the quarantined file.
4. Store both quarantined and archived data segregated by day-wise folders