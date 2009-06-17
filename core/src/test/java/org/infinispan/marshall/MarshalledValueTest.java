package org.infinispan.marshall;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.MarshalledValueInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ObjectDuplicator;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests implicit marshalled values
 *
 * @author Manik Surtani (<a href="mailto:manik AT jboss DOT org">manik AT jboss DOT org</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@Test(groups = "functional", testName = "marshall.MarshalledValueTest")
public class MarshalledValueTest extends MultipleCacheManagersTest {
   private Cache cache1, cache2;
   private MarshalledValueListenerInterceptor mvli;
   String k = "key", v = "value";
   private VersionAwareMarshaller marshaller;

   protected void createCacheManagers() throws Throwable {
      Configuration replSync = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      replSync.setUseLazyDeserialization(true);

      createClusteredCaches(2, "replSync", replSync);

      cache1 = cache(0, "replSync");
      cache2 = cache(1, "replSync");

      assertMarshalledValueInterceptorPresent(cache1);
      assertMarshalledValueInterceptorPresent(cache2);
   }

   private void assertMarshalledValueInterceptorPresent(Cache c) {
      InterceptorChain ic1 = TestingUtil.extractComponent(c, InterceptorChain.class);
      assert ic1.containsInterceptorType(MarshalledValueInterceptor.class);
   }

   @BeforeMethod
   public void addMarshalledValueInterceptor() {
      InterceptorChain chain = TestingUtil.extractComponent(cache1, InterceptorChain.class);
      chain.removeInterceptor(MarshalledValueListenerInterceptor.class);
      mvli = new MarshalledValueListenerInterceptor();
      chain.addInterceptorAfter(mvli, MarshalledValueInterceptor.class);
      
      marshaller = new VersionAwareMarshaller();
      marshaller.init(Thread.currentThread().getContextClassLoader(), null);
   }

   @AfterMethod
   public void tearDown() {
      Pojo.serializationCount = 0;
      Pojo.deserializationCount = 0;
   }

   private void assertOnlyOneRepresentationExists(MarshalledValue mv) {
      assert (mv.instance != null && mv.raw == null) || (mv.instance == null && mv.raw != null) : "Only instance or raw representations should exist in a MarshalledValue; never both";
   }

   private void assertSerialized(MarshalledValue mv) {
      assert mv.raw != null : "Should be serialized";
   }

   private void assertDeserialized(MarshalledValue mv) {
      assert mv.instance != null : "Should be deserialized";
   }

   private void assertSerializationCounts(int serializationCount, int deserializationCount) {
      assert Pojo.serializationCount == serializationCount : "Serialization count: expected " + serializationCount + " but was " + Pojo.serializationCount;
      assert Pojo.deserializationCount == deserializationCount : "Deserialization count: expected " + deserializationCount + " but was " + Pojo.deserializationCount;
   }

   public void testNonSerializable() {
      try {
         cache1.put("Hello", new Object());
         assert false : "Should have failed";
      }
      catch (CacheException expected) {

      }

      assert mvli.invocationCount == 0 : "Call should not have gone beyond the MarshalledValueInterceptor";

      try {
         cache1.put(new Object(), "Hello");
         assert false : "Should have failed";
      }
      catch (CacheException expected) {

      }

      assert mvli.invocationCount == 0 : "Call should not have gone beyond the MarshalledValueInterceptor";
   }

   public void testReleaseObjectValueReferences() {
      assert cache1.isEmpty();
      Pojo value = new Pojo();
      System.out.println(TestingUtil.extractComponent(cache1, InterceptorChain.class).toString());
      cache1.put("key", value);
      assert cache1.containsKey("key");
      assertSerializationCounts(1, 0);

      DataContainer dc1 = TestingUtil.extractComponent(cache1, DataContainer.class);

      InternalCacheEntry ice = dc1.get("key");
      Object o = ice.getValue();
      assert o instanceof MarshalledValue;
      MarshalledValue mv = (MarshalledValue) o;
      assertDeserialized(mv);
      assert cache1.get("key").equals(value);
      assertDeserialized(mv);
      assertSerializationCounts(1, 0);
      cache1.compact();
      assertSerializationCounts(2, 0);
      assertOnlyOneRepresentationExists(mv);
      assertSerialized(mv);

      // now on cache 2
      DataContainer dc2 = TestingUtil.extractComponent(cache2, DataContainer.class);
      ice = dc2.get("key");
      o = ice.getValue();
      assert o instanceof MarshalledValue;
      mv = (MarshalledValue) o;
      assertSerialized(mv); // this proves that unmarshalling on the recipient cache instance is lazy

      assert cache2.get("key").equals(value);
      assertDeserialized(mv);
      assertSerializationCounts(2, 1);
      cache2.compact();
      assertSerializationCounts(2, 1);
      assertOnlyOneRepresentationExists(mv);
      assertSerialized(mv);
   }

   public void testReleaseObjectKeyReferences() throws IOException, ClassNotFoundException {
      Pojo key = new Pojo();
      cache1.put(key, "value");

      assertSerializationCounts(1, 0);

      DataContainer dc1 = TestingUtil.extractComponent(cache1, DataContainer.class);

      Object o = dc1.keySet().iterator().next();
      assert o instanceof MarshalledValue;
      MarshalledValue mv = (MarshalledValue) o;
      assertDeserialized(mv);

      assert cache1.get(key).equals("value");
      assertDeserialized(mv);
      assertSerializationCounts(1, 0);

      cache1.compact();
      assertSerializationCounts(2, 0);
      assertOnlyOneRepresentationExists(mv);
      assertSerialized(mv);


      // now on cache 2
      DataContainer dc2 = TestingUtil.extractComponent(cache2, DataContainer.class);
      o = dc2.keySet().iterator().next();
      assert o instanceof MarshalledValue;
      mv = (MarshalledValue) o;
      assertSerialized(mv);
      assert cache2.get(key).equals("value");
      assertSerializationCounts(2, 1);
      assertDeserialized(mv);
      cache2.compact();

      assertOnlyOneRepresentationExists(mv);
      assertSerialized(mv);
      assertSerializationCounts(2, 1);
   }
   
   public void testKeySetValuesEntrySetCollectionReferences() {
      Pojo key1 = new Pojo(), value1 = new Pojo(), key2 = new Pojo(), value2 = new Pojo();
      String key3 = "3", value3 = "three"; 
      cache1.put(key1, value1);
      cache1.put(key2, value2);
      cache1.put(key3, value3);
      
      Set expKeys = new HashSet();
      expKeys.add(key1);
      expKeys.add(key2);
      expKeys.add(key3);
      
      Set expValues = new HashSet();
      expValues.add(value1);
      expValues.add(value2);
      expValues.add(value3);
      
      Set expKeyEntries = ObjectDuplicator.duplicateSet(expKeys);
      Set expValueEntries = ObjectDuplicator.duplicateSet(expValues);
      
      Set keys = cache2.keySet();
      for (Object key : keys) assert expKeys.remove(key);
      assert expKeys.isEmpty() : "Did not see keys " + expKeys + " in iterator!";
      
      Collection values = cache2.values();
      for (Object key : values) assert expValues.remove(key);
      assert expValues.isEmpty() : "Did not see keys " + expValues + " in iterator!";
      
      Set<Map.Entry> entries = cache2.entrySet();
      for (Map.Entry entry : entries) {
         assert expKeyEntries.remove(entry.getKey());
         assert expValueEntries.remove(entry.getValue());
      }
      assert expKeyEntries.isEmpty() : "Did not see keys " + expKeyEntries + " in iterator!";
      assert expValueEntries.isEmpty() : "Did not see keys " + expValueEntries + " in iterator!";
      
      Collection[] collections = new Collection[]{keys, values, entries};
      Object newObj = new Object();
      List newObjCol = new ArrayList();
      newObjCol.add(newObj);
      for (Collection col : collections) {
         try {
            col.add(newObj);
            assert false : "Should have thrown a UnsupportedOperationException";
         } catch (UnsupportedOperationException uoe) {
         }
         try {
            col.addAll(newObjCol);
            assert false : "Should have thrown a UnsupportedOperationException";
         } catch (UnsupportedOperationException uoe) {
         }
         
         try {
            col.clear();
            assert false : "Should have thrown a UnsupportedOperationException";
         } catch (UnsupportedOperationException uoe) {
         }
         
         try {
            col.remove(key1);
            assert false : "Should have thrown a UnsupportedOperationException";
         } catch (UnsupportedOperationException uoe) {
         }
         
         try {
            col.removeAll(newObjCol);
            assert false : "Should have thrown a UnsupportedOperationException";
         } catch (UnsupportedOperationException uoe) {
         }
         
         try {
            col.retainAll(newObjCol);
            assert false : "Should have thrown a UnsupportedOperationException";
         } catch (UnsupportedOperationException uoe) {
         }
      }
      
      for (Map.Entry entry : entries) {
         try {
            entry.setValue(newObj);
            assert false : "Should have thrown a UnsupportedOperationException";
         } catch (UnsupportedOperationException uoe) {
         }
      }
   }

   public void testEqualsAndHashCode() throws Exception {
      Pojo pojo = new Pojo();
      MarshalledValue mv = new MarshalledValue(pojo, true, marshaller);
      assertDeserialized(mv);
      int oldHashCode = mv.hashCode();

      mv.serialize();
      assertSerialized(mv);
      assert oldHashCode == mv.hashCode();

      MarshalledValue mv2 = new MarshalledValue(pojo, true, marshaller);
      assertSerialized(mv);
      assertDeserialized(mv2);

      assert mv2.hashCode() == oldHashCode;
      assert mv.equals(mv2);
   }

   public void assertUseOfMagicNumbers() throws Exception {
      Pojo pojo = new Pojo();
      MarshalledValue mv = new MarshalledValue(pojo, true, marshaller);


      VersionAwareMarshaller marshaller = new VersionAwareMarshaller();
      marshaller.init(Thread.currentThread().getContextClassLoader(), null);

      // start the test
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      ObjectOutput oo = marshaller.startObjectOutput(bout, false);
      marshaller.objectToObjectStream(mv, oo);
      marshaller.finishObjectOutput(oo);
      bout.close();

      // check that the rest just contains a byte stream which a MarshalledValue will be able to deserialize.
      ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
      ObjectInput oi = marshaller.startObjectInput(bin, false);
      MarshalledValue recreated = (MarshalledValue) marshaller.objectFromObjectStream(oi);

      // there should be nothing more
      assert oi.available() == 0;
      marshaller.finishObjectInput(oi);
      bin.close();

      assertSerialized(recreated);
      assert recreated.equals(mv);

      // since both objects being compared are serialized, the equals() above should just compare byte arrays.
      assertSerialized(recreated);
      assertOnlyOneRepresentationExists(recreated);
   }

   /**
    * Run this as last method as it creates and stops cache loaders, which might affect other tests.
    */
   @Test(dependsOnMethods = "org.infinispan.marshall.MarshalledValueTest.test(?!CacheLoaders)[a-zA-Z]*")
   public void testCacheLoaders() throws CloneNotSupportedException {
      tearDown();

      Configuration cacheCofig = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      cacheCofig.setUseLazyDeserialization(true);
      CacheLoaderManagerConfig clmc = new CacheLoaderManagerConfig();
      DummyInMemoryCacheStore.Cfg clc = new DummyInMemoryCacheStore.Cfg();
      clc.setStore(getClass().getSimpleName());
      clmc.setCacheLoaderConfigs(Collections.singletonList((CacheLoaderConfig) clc));
      cacheCofig.setCacheLoaderManagerConfig(clmc);

      defineCacheOnAllManagers("replSync2", cacheCofig);
      cache1 = cache(0, "replSync2");
      cache2 = cache(1, "replSync2");

      Pojo pojo = new Pojo();
      cache1.put("key", pojo);

      assertMarshalledValueInterceptorPresent(cache1);
      assertMarshalledValueInterceptorPresent(cache2);
      assertSerializationCounts(1, 0);

      cache2.get("key");

      assertSerializationCounts(1, 1);
   }

   public void testCallbackValues() {
      MockListener l = new MockListener();
      cache1.addListener(l);
      Pojo pojo = new Pojo();
      cache1.put("key", pojo);

      assert l.newValue != null;
      assert l.newValue instanceof MarshalledValue : "recieved " + l.newValue.getClass().getName();
      MarshalledValue mv = (MarshalledValue) l.newValue;
      assert mv.instance instanceof Pojo;
      assertSerializationCounts(1, 0);
   }

   public void testRemoteCallbackValues() throws Exception {
      MockListener l = new MockListener();
      cache2.addListener(l);
      Pojo pojo = new Pojo();
      cache1.put("key", pojo);

      assert l.newValue != null;
      assert l.newValue instanceof MarshalledValue;
      MarshalledValue mv = (MarshalledValue) l.newValue;
      assert mv.get() instanceof Pojo;
      assertSerializationCounts(1, 1);
   }

   @Listener
   public static class MockListener {
      Object newValue;

      @CacheEntryModified
      public void modified(CacheEntryModifiedEvent e) {
         if (!e.isPre()) newValue = e.getValue();
      }
   }

   class MarshalledValueListenerInterceptor extends CommandInterceptor {
      int invocationCount = 0;

      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         invocationCount++;
         if (command.getKey() instanceof MarshalledValue)
            assertOnlyOneRepresentationExists((MarshalledValue) command.getKey());
         if (command.getValue() instanceof MarshalledValue)
            assertOnlyOneRepresentationExists((MarshalledValue) command.getValue());
         Object retval = invokeNextInterceptor(ctx, command);
         if (retval instanceof MarshalledValue) assertOnlyOneRepresentationExists((MarshalledValue) retval);
         return retval;
      }

   }

   public static class Pojo implements Externalizable {
      int i;
      boolean b;
      static int serializationCount, deserializationCount;
      final Log log = LogFactory.getLog(Pojo.class);

      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Pojo pojo = (Pojo) o;

         if (b != pojo.b) return false;
         if (i != pojo.i) return false;

         return true;
      }

      public int hashCode() {
         int result;
         result = i;
         result = 31 * result + (b ? 1 : 0);
         return result;
      }

      public void writeExternal(ObjectOutput out) throws IOException {
         out.writeInt(i);
         out.writeBoolean(b);
         serializationCount++;
         log.trace("serializationCount=" + serializationCount);
      }

      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         i = in.readInt();
         b = in.readBoolean();
         deserializationCount++;
      }
   }
}
