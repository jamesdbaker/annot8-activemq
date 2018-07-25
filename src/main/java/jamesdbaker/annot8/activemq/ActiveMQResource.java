package jamesdbaker.annot8.activemq;

import io.annot8.core.components.Capabilities;
import io.annot8.core.components.Resource;
import io.annot8.core.context.Context;
import io.annot8.core.exceptions.BadConfigurationException;
import io.annot8.core.exceptions.MissingResourceException;
import io.annot8.core.settings.Settings;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.JMSException;

public class ActiveMQResource implements Resource {
  private Connection connection;

  @Override
  public void configure(Context context) throws BadConfigurationException, MissingResourceException {
    final ActiveMQResourceSettings settings = context.getSettings(ActiveMQResourceSettings.class);
    if(settings == null)
      throw new BadConfigurationException("Settings not found");

    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
    connectionFactory.setBrokerURL(settings.getBrokerUrl());

    try {
      connection = connectionFactory.createConnection();
      connection.start();
    }catch (Exception e){
      throw new BadConfigurationException("Unable to configure ActiveMQ connection", e);
    }
  }

  @Override
  public void close() {
    try {
      connection.close();
    } catch (JMSException e) {
      //TODO: Log error here
      e.printStackTrace();
    }
  }

  @Override
  public Capabilities getCapabilities() {
    //TODO:
    return null;
  }

  public Connection getConnection(){
    return connection;
  }

  public static class ActiveMQResourceSettings implements Settings {
    private String brokerUrl = "tcp://localhost:61616";

    public String getBrokerUrl() {
      return brokerUrl;
    }

    public void setBrokerUrl(String brokerUrl) {
      this.brokerUrl = brokerUrl;
    }
  }
}
