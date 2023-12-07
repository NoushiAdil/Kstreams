package com.services.streamsDemo.StreamsProcessor;

import com.services.streamsDemo.avro.schema.EmpFullDetails;
import com.services.streamsDemo.avro.schema.EmployeeAddress;
import com.services.streamsDemo.avro.schema.EmployeePersonal;

public class Streamjoins {
    public static EmpFullDetails setempFullDetails(EmployeePersonal personalKStream, EmployeeAddress addressKStream) {
        EmpFullDetails empFullDetails = new EmpFullDetails();
        if(personalKStream!=null) {
            empFullDetails.setEmpId(personalKStream.getEmpId());
            empFullDetails.setFirstName(personalKStream.getFirstName());
            empFullDetails.setLastName(personalKStream.getLastName());
            empFullDetails.setAge(personalKStream.getAge());
            empFullDetails.setSex(personalKStream.getSex());
        }
        if(addressKStream!=null) {
            empFullDetails.setEmpId(addressKStream.getEmpId());
            empFullDetails.setHouseName(addressKStream.getHouseName());
            empFullDetails.setStreetName(addressKStream.getStreetName());
            empFullDetails.setCity(addressKStream.getCity());
            empFullDetails.setPostcode(addressKStream.getPostcode());
            empFullDetails.setDistrict(addressKStream.getDistrict());
            empFullDetails.setState(addressKStream.getState());
            empFullDetails.setCountry(addressKStream.getCountry());
        }
        return empFullDetails;
    }


    public static EmpFullDetails aggregateSet(EmpFullDetails oldValue, EmpFullDetails newValue) {
        oldValue.setEmpId(newValue.getEmpId());
        oldValue.setFirstName(newValue.getFirstName());
        oldValue.setLastName(newValue.getLastName());
        oldValue.setAge(newValue.getAge());
        oldValue.setSex(newValue.getSex());
        oldValue.setHouseName(newValue.getHouseName());
        oldValue.setStreetName(newValue.getStreetName());
        oldValue.setCity(newValue.getCity());
        oldValue.setPostcode(newValue.getPostcode());
        oldValue.setDistrict(newValue.getDistrict());
        oldValue.setState(newValue.getState());
        oldValue.setCountry(newValue.getCountry());
        return oldValue;
    }
}
