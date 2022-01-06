package dto;

import org.apache.commons.lang.builder.ToStringBuilder;

public class FraudDto {
    String ipAddress;
    int nbLoginFailure = 0;
    String username;
    DeviceDto deviceDto;
    long time;


    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public int getNbLoginFailure() {
        return nbLoginFailure;
    }

    public void setNbLoginFailure(int nbLoginFailure) {
        this.nbLoginFailure = nbLoginFailure;
    }


    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public DeviceDto getDeviceDto() {
        return deviceDto;
    }

    public void setDeviceDto(DeviceDto deviceDto) {
        this.deviceDto = deviceDto;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

}
