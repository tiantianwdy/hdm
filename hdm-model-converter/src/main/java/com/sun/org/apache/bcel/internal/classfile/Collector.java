package com.sun.org.apache.bcel.internal.classfile;


import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.commons.Remapper;
import org.objectweb.asm.commons.RemappingClassAdapter;
import org.objectweb.asm.tree.ClassNode;

import java.io.IOException;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by tiantian on 14/07/15.
 */
public class Collector extends Remapper{

    private final Set<Class<?>> classNames;
    private final String prefix;

    public Collector(final Set<Class<?>> classNames, final String prefix){
        this.classNames = classNames;
        this.prefix = prefix;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String mapDesc(final String desc){
        if(desc.startsWith("L")){
            this.addType(desc.substring(1, desc.length() - 1));
        }
        return super.mapDesc(desc);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] mapTypes(final String[] types){
        for(final String type : types){
            this.addType(type);
        }
        return super.mapTypes(types);
    }

    private void addType(final String type){
        final String className = type.replace('/', '.');
        if(className.startsWith(this.prefix)){
            try{
                this.classNames.add(Class.forName(className));
            } catch(final ClassNotFoundException e){
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public String mapType(final String type){
        this.addType(type);
        return type;
    }



    public static Set<Class<?>> getClassesUsedBy(
            final String name,   // class name
            final String prefix  // common prefix for all classes
            // that will be retrieved
    ) throws IOException {
        final ClassReader reader = new ClassReader(name);
        final Set<Class<?>> classes =
                new TreeSet<Class<?>>(new Comparator<Class<?>>(){

                    @Override
                    public int compare(final Class<?> o1, final Class<?> o2){
                        return o1.getName().compareTo(o2.getName());
                    }
                });
        final Remapper remapper = new Collector(classes, prefix);
        final ClassVisitor inner = new ClassNode();
        final RemappingClassAdapter visitor =
                new RemappingClassAdapter(inner, remapper);
        reader.accept(visitor, 0);
        return classes;
    }
}