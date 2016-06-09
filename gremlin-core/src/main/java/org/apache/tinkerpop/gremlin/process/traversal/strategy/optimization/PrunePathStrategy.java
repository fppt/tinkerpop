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

import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Parameterizing;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
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
    private static final Set<Class<? extends OptimizationStrategy>> PRIORS = new HashSet<>();

    static {
        PRIORS.add(PathProcessorStrategy.class);
    }

    private PrunePathStrategy() {
    }

    public static PrunePathStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        TraversalParent parent = traversal.getParent();

        Set<String> foundLabels = new HashSet<>();
        Set<String> keepLabels = new HashSet<>();

        // If this traversal has a parent, it will need to inherit its
        // parent's keep labels.
        if(!parent.equals(EmptyStep.instance())) {
            // start with parents keep labels
            if(parent instanceof PathProcessor) {
                PathProcessor parentPathProcess = (PathProcessor) parent;
                if(parentPathProcess.getKeepLabels() != null) keepLabels.addAll(parentPathProcess.getKeepLabels());
            }
        }


        final List<Step> steps = traversal.getSteps();
        for(int i = steps.size() - 1; i >= 0; i--) {
            // maintain our list of labels to keep, repeatedly adding labels that were found during
            // the last iteration
            keepLabels.addAll(foundLabels);
            Step currentStep = steps.get(i);

            if(currentStep instanceof Parameterizing) {
                Parameters parameters = ((Parameterizing) currentStep).getParameters();
                for(Traversal.Admin trav : parameters.getTraversals()) {
                    for(Object ss : trav.getSteps()) {
                        if(ss instanceof Scoping) {
                            Set<String> labels = ((Scoping) ss).getScopeKeys();
                            for(String label : labels) {
                                if (foundLabels.contains(label)) {
                                    keepLabels.add(label);
                                } else {
                                    // it'll get added later, b/c we can drop after this step
                                    foundLabels.add(label);
                                }
                            }
                        }
                    }
                }
            }

            if(currentStep instanceof Scoping) {
                Set<String> labels = new HashSet<>(((Scoping) currentStep).getScopeKeys());
                if(currentStep instanceof MatchStep) {
                    labels.addAll(((MatchStep) currentStep).getMatchEndLabels());
                    labels.addAll(((MatchStep) currentStep).getMatchStartLabels());
                }
                for(final String label : labels) {
                    if(foundLabels.contains(label)) {
                        keepLabels.add(label);
                    } else {
                        foundLabels.add(label);
                    }
                }
            }

            if(currentStep instanceof PathProcessor) {
                        System.out.println(currentStep);
                        System.out.println(keepLabels);
                if(i != traversal.getSteps().size()) {
                    // add in all match labels
                    ((PathProcessor) currentStep).setKeepLabels(new HashSet<>(foundLabels));
                }
                ((PathProcessor) currentStep).setKeepLabels(new HashSet<>(keepLabels));
            }
        }
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return PRIORS;
    }
}
