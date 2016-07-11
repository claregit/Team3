


/********************************
**    Adjuster_Technician_v3   **
********************************/

/* 
Replace city names with zipcodes 
*/

/*
Sorting Adjuster Technician V2 table by adjuster and adjuster zip
*/

proc sort data=iaa.adjuster_technician_v2 out=iaa.SortedAdjZip;
	by adjuster adjuster_zip;
run;



/*
By Group Processing to output the first zipcode for each adjuster to make Adjuster_Zip dataset
the first entry for each ajuster seems to have a real zipcode
*/
data iaa.adjuster_zip;
	set iaa.SortedAdjzip;
	by adjuster adjuster_zip;
	if first.adjuster=1 and first.adjuster_zip=1 then do;
		adjuster=adjuster;
		verified_adjuster_zip=adjuster_zip;
		output;
		end;
	if last.adjuster;
	keep Adjuster Verified_Adjuster_zip;
run;




/*
Create Adjuster_zip_V2 where Adjuster_Technician_V2 was merged with Adjuster_Zip_V2
This puts Zipcode and Verified Zipcode in the same data set for replacement
*/


proc sql;
	create table iaa.adjuster_zip_v2 as
	select *
		from iaa.adjuster_technician_v2 as v,
			 iaa.adjuster_zip as z
		where v.adjuster=z.adjuster
		order by adjuster;
quit;




/*
Create Adjuster_Technician_V3 with correct zipcodes
*/


data iaa.adjuster_technician_v3;
	retain cov_id Adjuster Technician;
	set iaa.adjuster_zip_v2;
	if adjuster_zip=verified_adjuster_zip then zipcode=adjuster_zip;
	else if adjuster_zip ne verified_adjuster_zip then zipcode=verified_adjuster_zip;
	keep cov_id Adjuster Technician Zipcode;
	format Adjuster $7. Technician $7. zipcode 5.;
run;





/****************************************************
**    Customer Transaction Indicator Restructure   **
*****************************************************/


/*
fills in cov_limit and income for rewarded rows using retain statement
added Month and Year columns formatted Date in Date9
*/
data iaa.customer_trans_indicator_v2;
	set iaa.customer_transactions_indicator;
	retain _cov_limit;
	if not missing(cov_limit) then _cov_limit=cov_limit;
	else cov_limit=_cov_limit;
	drop _cov_limit;
	retain _income;
	if not missing(income) then _income=income;
	else income=_income;
	drop _income;
	Month=Month(date);
	Year=Year(date);
	format date date9.;
run;






/********************************
**    Rewarded (Death Table)   **
********************************/

/* 
From Customer Trans Indicator V2 - where transaction=RE - 53529 rows
*/



proc sql;
	create table iaa.rewarded as
	select date ,  Month, Year, cust_id, cov_id,income, transaction, type, reward_r, reward_a,
		   cov_limit, reward_trans, term_type, whole_type, variable_type, acc_reward,
		   crim_reward, health_reward, dan_ex_reward, war_ex_reward, av_ex_reward, 
		   s_ex_reward
		from iaa.customer_trans_indicator_v2
		where transaction='RE'
		order by date, cov_id;
quit;




/********************************
**         Rewarded_V2         **
********************************/

/*
Merge Rewarded with Adjuster_Technician_V3 - Who decided the Rewards?
*/

proc sql;
	create table iaa.rewarded_v2 as
	select *
		from iaa.rewarded as r,
			 iaa.adjuster_technician_v3 as a
		where r.cov_id=a.cov_id
		order by date, cov_id;


quit;






/******************************************************
**    Customer Trans IndicatorV2 Rollup on Cust_ID   **
******************************************************/

/*
Create indicator for when an award was rejected - different than a reward being missing
*/
data iaa.customer_trans_indicator_v3;
	set iaa.customer_trans_indicator_v2;
	if Reward_A = 0 then NotAwarded=1;
run;


/*
Created Summary table with rows grouped by cust_id
*/

proc sql;
	create table iaa.customer_transactions_summary as
	select cust_id,Count(cov_id) as Total_Transactions,
			sum(Reward_A) as Total_Amount_Rewarded, 
			count(Reward_A) as Total_Reward_Attempts,
			sum(NotAwarded) as Reward_Rejections,

			sum(initial_trans) as Total_Initial, sum(change_trans) as Total_Change,
				 sum(claim_trans) as Total_Claim,
			
				 max(cov_limit) as CovLim_Max, min(cov_limit) as CovLim_Min, 
				 mean(cov_limit) as CovLim_Mean, median(Cov_Limit) as CovLim_Median,

				 min(income) as Income_Min, max(income) as Income_Max,
			     mean(income) as Income_Mean, median(income) as Income_Median

				 
				 
				 
		from iaa.customer_trans_indicator_v3
		group by cust_id
		order by cust_id;
quit;




/******************************************************
**    Customer Medical From last Visit               **
******************************************************/

/*
Create a data set with coverage rewarded rows removed
	Table has 278838 rows,  add 53529 from rewarded and get 332367 for customer medical 
	as a check
*/

data iaa.customer_medical_WoRE;
	set iaa.customer_medical;
	where tobacco_num ne .
			and caffeine_num ne .
			and alcohol_num ne .
			and tobacco ne ' '
			and med_bp ne ' '
			and med_kid ne ' '
			and med_SE ne ' ';
run;



/*
Sort for by group processing
*/


proc sort data=iaa.customer_medical_WoRE out=sorted_cust_Med_WoRE;
	by cust_id cov_id;
run;


/*
Create Customer_medical_last for overall merge with information from the customers last visit
*/

data iaa.customer_medical_last;
	set sorted_cust_med_WoRE;
	by cust_id cov_id;
	if last.cust_id=1 and last.cov_id=1 then output;
run;


/*
Create indicator variables for summary
*/


proc sql;
	create table vars as
		select varnum, name, cat('I_',name) as Indicator
		from dictionary.columns
		where memname='CUSTOMER_MEDICAL_LAST';
quit;

proc sql;
	select name
			into :medvars separated by ' '
	from vars
	where varnum not in (1,2,3,4,5,6,7,8,9);
	select indicator
			into  :ind separated by ' '
	from vars
	where varnum not in (1,2,3,4,5,6,7,8,9);
quit;

%put fmedvars is &medvars;
%put ind is &ind;


	
data iaa.Customer_Medical_Last_V2;
	set iaa.Customer_Medical_Last;
	array vars{*} $ &medvars;
	array ind{*}  &ind;
	do i=1 to dim(ind);
		if vars{i}='Y' then ind{i}=1;
		else ind{i} = 0;
	end;
	drop i;
	Total_Indications=sum(of I_Med:);
	
run;





/******************************************************
**                  Overall Table                    **
******************************************************/


/*
Merge Customer_info_v2
	  Customer_Transactions_Summary
	  Customer_Medical_Last_v2
	  Customer_Family_Medical_V2
*/


proc sql;
	create table iaa.Overall_Cust as
	select i.cust_id, *
		from iaa.customer_info_v2 as i,
			 iaa.customer_transactions_summary as t,
			 iaa.customer_medical_last_v2 as m,
			 iaa.customer_family_medical_v2 as f
		where i.cust_id=t.cust_id and
			  i.cust_id=m.cust_id and
			  i.cust_id=f.cust_id
		order by cust_id;
quit;


/*
Prune Information
*/

proc sql;
	create table iaa.Overall_Cust_Reduced as

		select cust_id, fullname, streetaddress, city, state, zipcode, country,
			 telephonenumber, MothersMaiden, birthday, age, cctype, nationalid,
			 vehicleYear, vehicleMake, vehicleModel, bloodtype, pounds, height, bmi,
			 gender, race, marriage, 

			 total_transactions, total_amount_rewarded, total_reward_attempts,reward_rejections,
			 total_initial, total_change, total_claim, 

			 covlim_max, covlim_min, covlim_mean, covlim_median, income_min, income_max, income_mean, income_median,
			 date as Date_Last_Visit, cov_id, Tobacco_Num, Caffeine_Num, Alcohol_Num, 

			 Total_Cust_Indications, Total_Fam_Indications, Total_Mother_Ind, Total_Father_ind,


			I_Med_HA, I_Med_BP, I_Med_Can, I_Med_Diab, I_Med_Chol, I_Med_Arth, I_Med_Asth, I_Med_Gla, I_Med_Kid, I_Med_Leuk,
			I_Med_Ment, I_Med_SE, I_Med_SCA, I_Med_Str, I_Med_TD, I_Med_TB, I_Med_Ul, 

			I_MedF_HA, I_MedF_BP, I_MedF_Can, I_MedF_Diab, I_MedF_Chol, I_MedF_Arth, I_MedF_Asth, I_MedF_Gla, I_MedF_Kid, I_MedF_Leuk,
			I_MedF_Ment, I_MedF_SE, I_MedF_SCA, I_MedF_Str, I_MedF_TD, I_MedF_TB, I_MedF_Ul, 

			I_MedM_HA, I_MedM_BP, I_MedM_Can, I_MedM_Diab, I_MedM_Chol, I_MedM_Arth, I_MedM_Asth, I_MedM_Gla, I_MedM_Kid, I_MedM_Leuk,
			I_MedM_Ment, I_MedM_SE, I_MedM_SCA, I_MedM_Str, I_MedM_TD, I_MedM_TB, I_MedM_Ul

		from iaa.overall_cust;

	quit;
