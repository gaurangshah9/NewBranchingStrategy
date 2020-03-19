    read -p "Please provide retailer name like asda, tesco, walmart etc in lower case:" retailerName
    read -p "Please provide client name like clorox, barilla etc in lower case:" clientName
    read -p "Please provide country code name like us, uk etc in lower case:" countryCode
    read -p "Please provide StatusLOESS (ON or OFF):" StatusLOESS
    read -p "Please provide StatusDRFE (ON or OFF):" StatusDRFE

#Add new triggers
sed -e "s/\${client}/${clientName}/g" -e "s/\${retailer}/${retailerName}/g" -e "s/\${country_cd}/${countryCode}/g" trigger-template.json > "trigger-${retailerName}-${clientName}-${countryCode}.json"
mv -v "trigger-${retailerName}-${clientName}-${countryCode}.json" ../ADF/trigger/
echo "added trigger-${retailerName}-${clientName}-${countryCode}.json"

#Add config files
sed -e "s/\${countryCode}/${countryCode}/g" -e "s/\${StatusLOESS}/${StatusLOESS}/g" -e "s/\${StatusDRFE}/${StatusDRFE}/g" param_file_template.json > "params_alert_generation_${retailerName}_${clientName}_${countryCode}.json"
mv -v "params_alert_generation_${retailerName}_${clientName}_${countryCode}.json" ../config/
echo "added ../config/params_alert_generation_${retailerName}_${clientName}_${countryCode}.json"

## modify client_list.txt
if grep -q ${retailerName}_${clientName}_${countryCode} ../client_config/client_list.txt
then
	echo "client already exist"
else
	echo "${retailerName}_${clientName}_${countryCode}">>  ../client_config/client_list.txt
	echo "Added ${retailerName}_${clientName}_${countryCode} name to ../client_config/client_list.txt"
fi
