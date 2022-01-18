package dto;

import org.apache.commons.lang.builder.ToStringBuilder;

public class DeviceDto {
    public static final String UNKNOWN = "Unknown";
    private static final String OTHER = "Other";
    private static final String BROWSER_VERSION_SEPARATOR = "/";

    public static DeviceDto unknown() {
        DeviceDto device = new DeviceDto();

        device.setOs(OTHER);
        device.setDevice(OTHER);

        return device;
    }

    private String ipAddress;
    private String os;
    private String osVersion;
    private String browser;
    private String device;
    private boolean mobile;
    private boolean isKnown = false;



    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ip) {
        this.ipAddress = ip;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public String getOsVersion() {
        if (osVersion == null) {
            return UNKNOWN;
        }
        return osVersion;
    }

    public void setOsVersion(String osVersion) {
        this.osVersion = osVersion;
    }

    public String getBrowser() {
        return browser;
    }

    public void setBrowser(String browser) {
        this.browser = browser;
    }

    public void setBrowser(String browser, String version) {
        if (browser == null) {
            this.browser = OTHER;
        } else {
            this.browser = new StringBuilder(browser).append(BROWSER_VERSION_SEPARATOR).append(version == null ? UNKNOWN : version).toString();
        }
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }


    public void setMobile(boolean mobile) {
        this.mobile = mobile;
    }

    public boolean isMobile() {
        return mobile;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

}
