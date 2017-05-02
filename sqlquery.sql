create view city_dist as
select c1.CountryID,
	c1.CityId, c1.City,
	c2.CityId as CityId2, c2.City as City2,
	sqrt(pow(c1.Latitude-c2.Latitude,2) + pow(c1.Longitude-c2.Longitude,2)) as Dist
from cities c1 inner join cities c2 on c1.CountryID = c2.CountryID
where   c1.CityId < c2.CityId  
and c1.CountryID <> 254 



select city_dist.*
from (
	select CountryID, min(Dist) as MinDist
	from city_dist
	where Dist > 0 
	group by CountryID
) a inner join city_dist on a.CountryID = city_dist.CountryID where a.MinDist = city_dist.Dist;
