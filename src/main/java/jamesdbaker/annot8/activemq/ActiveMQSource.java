package jamesdbaker.annot8.activemq;

import io.annot8.common.content.Text;
import io.annot8.core.components.Capabilities;
import io.annot8.core.components.Source;
import io.annot8.core.components.responses.SourceResponse;
import io.annot8.core.context.Context;
import io.annot8.core.data.Item;
import io.annot8.core.data.ItemFactory;
import io.annot8.core.exceptions.BadConfigurationException;
import io.annot8.core.exceptions.IncompleteException;
import io.annot8.core.exceptions.MissingResourceException;
import io.annot8.core.exceptions.UnsupportedContentException;
import io.annot8.core.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;

public class ActiveMQSource implements Source {
  private List<Message> messages = new ArrayList<>();

  private Session session = null;

  private Logger LOGGER = LoggerFactory.getLogger(ActiveMQSource.class);

  @Override
  public SourceResponse read(ItemFactory itemFactory) {
    if(messages.isEmpty())
      return SourceResponse.empty();

    messages.forEach(m -> {
      final Item item = itemFactory.create();

      try {
        Enumeration<String> e = (Enumeration<String>) m.getPropertyNames();
        while (e.hasMoreElements()) {
          String name = e.nextElement();
          item.getProperties().set(name, m.getObjectProperty(name));
        }

        item.getProperties().set("_jms.expiration", m.getJMSExpiration());
        item.getProperties().set("_jms.message_id", m.getJMSMessageID());
        item.getProperties().set("_jms.priority", m.getJMSPriority());
        item.getProperties().set("_jms.type", m.getJMSType());
        item.getProperties().set("_jms.timestamp", m.getJMSTimestamp());
        //TODO: Are any of the other fields useful?
      }catch (JMSException e) {
        LOGGER.warn("Exception caught whilst adding to properties to item", e);
        item.discard();
      }

      if(m instanceof TextMessage) {
        try {
          item.create(Text.class)
              .withData(((TextMessage) m).getText())
              .withName("message")
              .save();
        }catch (UnsupportedContentException | IncompleteException | JMSException e) {
          LOGGER.warn("Exception caught whilst creating Text content", e);
          item.discard();
        }
      }else{
          LOGGER.debug("Unsupported message type {}", m.getClass().getName());
          item.discard();
      }

    });

    messages.clear();

    return SourceResponse.ok();
  }

  @Override
  public void configure(Context context) throws BadConfigurationException, MissingResourceException {
    final ActiveMQSourceSettings settings = context.getSettings(ActiveMQSourceSettings.class);
    if(settings == null)
      throw new BadConfigurationException("Settings not found");

    final Optional<ActiveMQResource> resource = context.getResource(ActiveMQResource.class);
    if(!resource.isPresent()){
      throw new MissingResourceException("ActiveMQ connection not found");
    }

    try{
    session = resource.get().getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
    MessageConsumer consumer = session.createConsumer(session.createTopic(settings.getTopicName()));
    consumer.setMessageListener(new Annot8MessageListener());
    }catch (JMSException e){
      throw new BadConfigurationException("Unable to connect to topic", e);
    }
  }

  @Override
  public void close() {
    if(session != null){
      try {
        session.close();
      } catch (JMSException e) {
        LOGGER.debug("Exception caught whilst closing ActiveMQ session");
      }
    }
  }

  @Override
  public Capabilities getCapabilities() {
    return null;
  }

  private class Annot8MessageListener implements MessageListener {
    @Override
    public void onMessage(Message message) {
      messages.add(message);
    }
  }

  public static class ActiveMQSourceSettings implements Settings{
    private String topicName;

    public String getTopicName() {
      return topicName;
    }

    public void setTopicName(String topicName) {
      this.topicName = topicName;
    }
  }
}
