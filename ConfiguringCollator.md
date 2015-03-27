# Introduction #

This is a place holder document under construction.
Should probably mention nullsAreSortedLow property, too.


# Details #

```
    <!--                                                                               
      A java.text.Collator implementation that mimics the sorting done by              
      the database ORDER BY clause specified in the Traversal SQL query                
      for text values (CHAR, VARCHAR, etc).                                            
      By default, the connector uses the Collator for the current default              
      locale, with TERTIARY strength (case and accent sensitive), and                  
      CANONICAL_DECOMPOSITION of accented characters.                                  
    -->
    <!--                                                                               
      This example uses the java.text.Collator static factory method to                
      get a Collator for the current default locale, then set the strength              
      to be case-insensitive and accent-insensitive.                                   
    -->
    <!--                                                                               
    <property name="collator">                                                         
      <bean class="java.text.Collator" factory-method="getInstance">                   
        <property name="strength">                                                     
          <util:constant static-field="java.text.Collator.PRIMARY"/>                   
        </property>                                                                    
        <property name="decomposition">                                                
          <util:constant static-field="java.text.Collator.CANONICAL_DECOMPOSITION"/>   
        </property>                                                                    
      </bean>                                                                          
    </property>                                                                        
    -->

    <!--                                                                               
      This example uses the java.text.Collator static factory method to                
      get the specified locale, then set the strength to be                            
      case-insensitive but accent-sensitive.                                           
    -->
    <!--                                                                               
    <property name="collator">                                                         
      <bean class="java.text.Collator" factory-method="getInstance">                   
        <constructor-arg>                                                              
          <bean class="java.util.Locale">                                              
            <constructor-arg value="fre"/>                                             
            <constructor-arg value="fr"/>                                              
          </bean>                                                                      
        </constructor-arg>                                                             
        <property name="strength">                                                     
          <util:constant static-field="java.text.Collator.SECONDARY"/>                 
        </property>                                                                    
        <property name="decomposition">                                                
          <util:constant static-field="java.text.Collator.CANONICAL_DECOMPOSITION"/>   
        </property>                                                                    
      </bean>                                                                          
    </property>                                                                        
    -->

    <!--                                                                               
      This example configures a brain-dead java.text.RuleBasedCollator.                
    -->
    <!--                                                                               
    <property name="collator">                                                         
      <bean class="java.text.RuleBasedCollator">                                       
        <constructor-arg value="< a,A< b,B< c,C< d,D< e,E< f,F< g,G< h,H< i,I< j,J < k,K< l,L< m,M< n,N< o,O< p,P< q,Q< r,R< s,S< t,T < u,U< v,V< w,W< x,X< y,Y< z,Z"/>      
      </bean>                                                                          
    </property>                                                                        
    -->

    <!--                                                                               
      This example configures a collator that performs collation using a SQL query.                 
    -->
    <!--                                                                                                                                                         
    <property name="collator">                                                                                                                                   
      <bean class="com.google.enterprise.connector.db.SqlCollator">                                                                                              
        <property name="collatorId" value="Latin1_General_CS_AI"/>                                                                                               
      </bean>                                                                                                                                                    
    </property>                                                                                                                                                  
    -->

```