/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Ted Wilmes (http://twilmes.org)
 */
public final class PrunePathStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final PrunePathStrategy INSTANCE = new PrunePathStrategy();

    private PrunePathStrategy() {
    }

    public static PrunePathStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {

        if(!traversal.getParent().equals(EmptyStep.instance())) {
            // start with parents keep labels
            if(parent instanceof PathProcessor) {
                (())
            }
        }

        Set<String> foundLabels = new HashSet<>();
        final List<Step> steps = traversal.getSteps();
        
        try {
            Set<String> keepLabels = new HashSet<>();
            for(int i = steps.size() - 1; i >= 0; i--) {
                Step currentStep = steps.get(i);
                if(currentStep instanceof Scoping) {
                    Set<String> labels = ((Scoping) currentStep).getScopeKeys();
                    for(final String label : labels) {
                        if(foundLabels.contains(label)) {
                            keepLabels.add(label);
                        } else {
                            // it'll get added later, b/c we can drop after this step
                            foundLabels.add(label);
                        }
                    }
                    if(currentStep instanceof PathProcessor) {
                        ((PathProcessor) currentStep).setKeepLabels(keepLabels);
                    }
                    keepLabels.addAll(foundLabels);
                }
//                Step currentStep = steps.get(i);
//                Set<String> labels = new HashSet<>();
//                labels.addAll(currentStep.getLabels());
//                if(currentStep instanceof Scoping) {
//                    labels.addAll(((Scoping) currentStep).getScopeKeys());
//                }
//                Set<String> dropThese = new HashSet<>();
//                for(final String label : labels) {
//                    if(!foundLabels.contains(label)) {
//                        dropThese.add(label);
//                        foundLabels.add(label);
//                    }
//                }
//
//                if(!dropThese.isEmpty()) {
////                    TraversalHelper.insertAfterStep(
////                            new PrunePathStep<>(traversal, true, dropThese.toArray(new String[dropThese.size()])), currentStep, traversal.asAdmin());
//                    if(currentStep instanceof PathProcessor) {
//                        ((PathProcessor) currentStep).setKeepLabels(dropThese);
//                    }
//                }
            }
        } catch (FastNoSuchElementException e) {

        }



        // insert prunePathStep after max step
//        for (final Map.Entry<String, Integer> entry : labelMaxStepMap.entrySet()) {
////            List<String> labels = entry.getValue();
////            if (!labels.isEmpty()) {
//                Step step = entry.getKey();
//                if(! (step.getNextStep() instanceof EmptyStep)) {
//                    // if there aren't any labels to drop after this, go ahead and
//                    // drop the full path and skip the labels
//                    boolean dropPath = true;
//                    TraversalHelper.insertAfterStep(new PrunePathStep<>(traversal, dropPath, labels.toArray(new String[labels.size()])), step, traversal.asAdmin());
//                }
//            }
//        }
    }
}
